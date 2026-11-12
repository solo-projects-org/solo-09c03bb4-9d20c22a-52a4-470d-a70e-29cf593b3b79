"""Whole-day reconciliation of a billing CSV against an Iceberg v2 snapshot.

The voyage-day check answers four bookkeeping questions:

* 错引 (wrong reference): a billing row points at a ``(file_path, pos)`` that
  does not exist in the data files, or the ticket number of the source row
  does not match the billing row.
* 误计 (mischarge): the billing ticket matches the source row, but that source
  row is already removed by an in-scope Iceberg positional delete.
* 重计 (recharge): one *live* source row has more than one ticket-matching
  billing row; every such billing row is reported.
* 漏计 (missing charge): a *live* source row has no ticket-matching billing
  row at all.

The existing positional-delete rule is reused verbatim: a delete record
applies only when its ``file_path`` equals the data file's original path,
``pos`` equals the zero-based row index and the delete file's data sequence
number is not smaller than the data file's.

Streaming contract
------------------
Nothing for the whole voyage day is accumulated in Python memory:

* Parquet files are read one row group at a time and spooled into a
  temporary, file-backed SQLite database in fixed-size batches.
* The billing CSV is parsed line by line; validated rows are spooled in
  batches.
* Two batched cursor passes produce the differences; report rows are
  streamed straight to the output CSV.

Any equality-delete manifest entry, missing replica, unreadable file, bad
billing row or local read/write failure aborts the whole run with a
``CheckError``; a partially written report is removed and no download is
offered.
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow.parquet as pq

from .engine import (
    DELETE_PATH_COLUMN,
    DELETE_POS_COLUMN,
    _find_parquet_column,
    _require_integer_column,
    _require_string_column,
)
from .errors import CheckError
from .manifest import Manifest

# Billing CSV schema (fixed by the voyage-day check specification).
BILLING_TICKET_COLUMN = "ticket_no"
BILLING_PATH_COLUMN = "file_path"
BILLING_POS_COLUMN = "pos"
BILLING_COLUMNS = (
    BILLING_TICKET_COLUMN,
    BILLING_PATH_COLUMN,
    BILLING_POS_COLUMN,
)

# Difference categories (Chinese labels are also used verbatim in the report).
DIFF_WRONG_REF = "错引"
DIFF_MISCHARGE = "误计"
DIFF_RECHARGE = "重计"
DIFF_MISSING = "漏计"
DIFF_TYPES = (
    DIFF_WRONG_REF,
    DIFF_MISCHARGE,
    DIFF_RECHARGE,
    DIFF_MISSING,
)

REPORT_HEADER = (
    "diff_type",
    "billing_row_no",
    "billing_ticket_no",
    "file_path",
    "pos",
    "source_ticket_no",
    "detail",
)

BATCH_SIZE = 4096


@dataclass
class ReconcileResult:
    manifest: Manifest
    ticket_column: str
    billing_row_count: int
    source_row_count: int
    diff_counts: dict[str, int]
    report_path: Path
    warnings: list[str] = field(default_factory=list)
    report_token: str | None = None

    @property
    def total_diffs(self) -> int:
        return sum(self.diff_counts.values())


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _parse_non_negative_int(text: str) -> int:
    text = (text or "").strip()
    if not text or not text.lstrip("+").isdigit():
        raise ValueError("不是非负整数")
    return int(text)


def _empty_counts() -> dict[str, int]:
    return {kind: 0 for kind in DIFF_TYPES}


def _open_spool(path: Path) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(str(path))
        connection.execute("PRAGMA journal_mode=MEMORY")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=MEMORY")
        # Bound the page cache (~20 MiB); the spool itself stays on disk.
        connection.execute("PRAGMA cache_size=-20000")
    except sqlite3.Error as exc:
        raise CheckError(f"无法创建本地临时核验数据库：{exc}") from None
    connection.executescript(
        """
        CREATE TABLE source (
            file_path TEXT NOT NULL,
            pos INTEGER NOT NULL,
            ticket_no INTEGER NOT NULL
        );
        CREATE TABLE billing (
            row_no INTEGER NOT NULL,
            ticket_no INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            pos INTEGER NOT NULL
        );
        CREATE TABLE pos_delete (
            file_path TEXT NOT NULL,
            pos INTEGER NOT NULL,
            seq INTEGER NOT NULL
        );
        """
    )
    return connection


def _insert_batch(
    connection: sqlite3.Connection,
    statement: str,
    batch: list[tuple],
    *,
    label: str,
) -> None:
    if not batch:
        return
    try:
        connection.executemany(statement, batch)
        connection.commit()
    except sqlite3.Error as exc:
        raise CheckError(f"写入本地临时数据失败（{label}）：{exc}") from None
    batch.clear()


# ---------------------------------------------------------------------------
# Preflight: manifest shape and Parquet replicas
# ---------------------------------------------------------------------------


def _check_presence(
    manifest: Manifest, uploaded_paths: Mapping[str, Path]
) -> None:
    missing = [
        entry.uploaded_filename
        for entry in manifest.required_entries
        if entry.uploaded_filename not in uploaded_paths
        or not Path(uploaded_paths[entry.uploaded_filename]).is_file()
    ]
    if missing:
        raise CheckError(
            "清单中以下文件的上传副本缺失，无法进行整日核验。",
            files=missing,
        )


def _spool_data_files(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    connection: sqlite3.Connection,
    ticket_column: str,
) -> int:
    """Read every data file row-group by row-group and spool all source rows."""
    source_rows = 0
    batch: list[tuple] = []
    for entry in manifest.data_files:
        label = entry.uploaded_filename
        try:
            parquet = pq.ParquetFile(str(uploaded_paths[label]))
        except Exception as exc:
            raise CheckError(
                f"无法读取数据文件 “{label}”（原始路径 {entry.original_path}）："
                f"{exc}",
                files=[label],
            ) from None

        _require_integer_column(
            parquet.schema_arrow,
            ticket_column,
            file_label=label,
            role="数据文件",
        )
        column_index = _find_parquet_column(parquet.metadata, ticket_column)
        if column_index is None:
            raise CheckError(
                f"数据文件 “{label}” 的 Parquet 元数据中找不到列 "
                f"“{ticket_column}”。",
                files=[label],
            )

        metadata = parquet.metadata
        start_row = 0
        for row_group_index in range(metadata.num_row_groups):
            num_rows = int(metadata.row_group(row_group_index).num_rows)
            try:
                table = parquet.read_row_group(
                    row_group_index, columns=[ticket_column]
                )
            except Exception as exc:
                raise CheckError(
                    f"读取数据文件 “{label}”（原始路径 "
                    f"{entry.original_path}）的行组 {row_group_index} 失败："
                    f"{exc}",
                    files=[label],
                ) from None
            if table.num_rows != num_rows:
                raise CheckError(
                    f"数据文件 “{label}” 的行组 {row_group_index} 元数据说 "
                    f"{num_rows} 行，实际读取 {table.num_rows} 行。无法可靠"
                    "计算文件内行号。",
                    files=[label],
                )
            tickets = table.column(ticket_column).to_pylist()
            for offset, ticket in enumerate(tickets):
                if ticket is None or not isinstance(ticket, int):
                    raise CheckError(
                        f"数据文件 “{label}” 第 {start_row + offset} 行"
                        "（从 0 起算）的票号列为空或不是整数，属于坏行。",
                        files=[label],
                    )
                batch.append(
                    (entry.original_path, start_row + offset, ticket)
                )
                if len(batch) >= BATCH_SIZE:
                    _insert_batch(
                        connection,
                        "INSERT INTO source(file_path, pos, ticket_no)"
                        " VALUES (?, ?, ?)",
                        batch,
                        label=label,
                    )
                source_rows += 1
            start_row += num_rows

    _insert_batch(
        connection,
        "INSERT INTO source(file_path, pos, ticket_no) VALUES (?, ?, ?)",
        batch,
        label="source",
    )
    return source_rows


def _spool_position_deletes(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    connection: sqlite3.Connection,
) -> None:
    """Stream positional delete files through the same delete-rule columns."""
    batch: list[tuple] = []
    for entry in manifest.position_delete_files:
        label = entry.uploaded_filename
        try:
            parquet = pq.ParquetFile(str(uploaded_paths[label]))
            _require_string_column(
                parquet.schema_arrow,
                DELETE_PATH_COLUMN,
                file_label=label,
            )
            _require_integer_column(
                parquet.schema_arrow,
                DELETE_POS_COLUMN,
                file_label=label,
                role="删除文件",
            )
        except CheckError:
            raise
        except Exception as exc:
            raise CheckError(
                f"无法读取位置删除文件 “{label}”（原始路径 "
                f"{entry.original_path}）：{exc}",
                files=[label],
            ) from None

        row_offset = 0
        for row_group_index in range(parquet.metadata.num_row_groups):
            try:
                table = parquet.read_row_group(
                    row_group_index,
                    columns=[DELETE_PATH_COLUMN, DELETE_POS_COLUMN],
                )
            except Exception as exc:
                raise CheckError(
                    f"读取位置删除文件 “{label}” 的行组 "
                    f"{row_group_index} 失败：{exc}",
                    files=[label],
                ) from None
            paths = table.column(DELETE_PATH_COLUMN).to_pylist()
            positions = table.column(DELETE_POS_COLUMN).to_pylist()
            for offset, (record_path, record_pos) in enumerate(
                zip(paths, positions)
            ):
                row_index = row_offset + offset
                if record_path is None or not isinstance(record_path, str) or record_path == "":
                    raise CheckError(
                        f"位置删除文件 “{label}” 第 {row_index} 行"
                        "（从 0 起算）的 file_path 为空或不是字符串。",
                        files=[label],
                    )
                if record_pos is None or not isinstance(record_pos, int):
                    raise CheckError(
                        f"位置删除文件 “{label}” 第 {row_index} 行"
                        "（从 0 起算）的 pos 不是整数。",
                        files=[label],
                    )
                if record_pos < 0:
                    raise CheckError(
                        f"位置删除文件 “{label}” 第 {row_index} 行"
                        f"（从 0 起算）的 pos={record_pos} 为负数。",
                        files=[label],
                    )
                batch.append(
                    (record_path, record_pos, entry.sequence_number)
                )
                if len(batch) >= BATCH_SIZE:
                    _insert_batch(
                        connection,
                        "INSERT INTO pos_delete(file_path, pos, seq)"
                        " VALUES (?, ?, ?)",
                        batch,
                        label=label,
                    )
            row_offset += table.num_rows

    _insert_batch(
        connection,
        "INSERT INTO pos_delete(file_path, pos, seq) VALUES (?, ?, ?)",
        batch,
        label="pos_delete",
    )


# ---------------------------------------------------------------------------
# Billing CSV validation + spool
# ---------------------------------------------------------------------------


def _spool_billing_csv(
    billing_csv: Path,
    billing_label: str,
    connection: sqlite3.Connection,
) -> int:
    try:
        handle = open(
            billing_csv, "r", encoding="utf-8-sig", newline=""
        )
    except OSError as exc:
        raise CheckError(
            f"无法打开计费 CSV “{billing_label}”：{exc}",
            files=[billing_label],
        ) from None

    batch: list[tuple] = []
    row_count = 0
    with handle:
        try:
            reader = csv.DictReader(handle)
        except csv.Error as exc:
            raise CheckError(
                f"计费 CSV “{billing_label}” 无法解析：{exc}",
                files=[billing_label],
            ) from None

        raw_headers = reader.fieldnames
        if not raw_headers:
            raise CheckError(
                f"计费 CSV “{billing_label}” 为空或缺少表头。表头必须包含 "
                "ticket_no、file_path、pos。",
                files=[billing_label],
            )
        headers = [h.strip() if isinstance(h, str) else h for h in raw_headers]
        if len(set(headers)) != len(headers):
            raise CheckError(
                f"计费 CSV “{billing_label}” 的表头出现重复列名。",
                files=[billing_label],
            )
        missing = [name for name in BILLING_COLUMNS if name not in headers]
        if missing:
            raise CheckError(
                f"计费 CSV “{billing_label}” 缺少必需列："
                + "、".join(missing)
                + "。表头必须包含 ticket_no、file_path、pos。",
                files=[billing_label],
            )

        try:
            for raw_row in reader:
                line_num = reader.line_num
                if raw_row is None:
                    continue
                row = {
                    (k.strip() if isinstance(k, str) else k): v
                    for k, v in raw_row.items()
                    if k is not None
                }
                # Blank lines produce rows whose actual columns are all empty;
                # only the three required columns participate in the test.
                if not any(
                    (row.get(name) or "").strip()
                    for name in BILLING_COLUMNS
                ):
                    # Fully blank line: skip without consuming a data row number.
                    continue
                row_count += 1
                row_no = row_count
                location = (
                    f"计费 CSV “{billing_label}” 第 {row_no} 条数据行"
                    f"（文件第 {line_num} 行）"
                )

                ticket_text = (row.get(BILLING_TICKET_COLUMN) or "").strip()
                path_text = (row.get(BILLING_PATH_COLUMN) or "").strip()
                pos_text = (row.get(BILLING_POS_COLUMN) or "").strip()
                if not ticket_text:
                    raise CheckError(
                        f"{location}的 ticket_no 为空，属于坏行。",
                        files=[billing_label],
                    )
                if not path_text:
                    raise CheckError(
                        f"{location}的 file_path 为空，属于坏行。",
                        files=[billing_label],
                    )
                if not pos_text:
                    raise CheckError(
                        f"{location}的 pos 为空，属于坏行。",
                        files=[billing_label],
                    )
                try:
                    ticket = _parse_non_negative_int(ticket_text)
                except ValueError:
                    raise CheckError(
                        f"{location}的 ticket_no “{ticket_text}” "
                        "不是非负整数，属于坏行。",
                        files=[billing_label],
                    ) from None
                try:
                    pos = _parse_non_negative_int(pos_text)
                except ValueError:
                    raise CheckError(
                        f"{location}的 pos “{pos_text}” "
                        "不是非负整数，属于坏行。",
                        files=[billing_label],
                    ) from None

                batch.append((row_no, ticket, path_text, pos))
                if len(batch) >= BATCH_SIZE:
                    _insert_batch(
                        connection,
                        "INSERT INTO billing(row_no, ticket_no, file_path, pos)"
                        " VALUES (?, ?, ?, ?)",
                        batch,
                        label=billing_label,
                    )
        except csv.Error as exc:
            raise CheckError(
                f"计费 CSV “{billing_label}” 解析失败：{exc}",
                files=[billing_label],
            ) from None

    _insert_batch(
        connection,
        "INSERT INTO billing(row_no, ticket_no, file_path, pos)"
        " VALUES (?, ?, ?, ?)",
        batch,
        label=billing_label,
    )
    return row_count


# ---------------------------------------------------------------------------
# Classification passes
# ---------------------------------------------------------------------------


def _write_report_row(
    writer: csv.writer,
    counts: dict[str, int],
    diff_type: str,
    *,
    billing_row_no: int | None,
    billing_ticket: int | None,
    file_path: str,
    pos: int | None,
    source_ticket: int | None,
    detail: str,
) -> None:
    writer.writerow(
        [
            diff_type,
            "" if billing_row_no is None else billing_row_no,
            "" if billing_ticket is None else billing_ticket,
            file_path,
            "" if pos is None else pos,
            "" if source_ticket is None else source_ticket,
            detail,
        ]
    )
    counts[diff_type] += 1


def _classify(
    connection: sqlite3.Connection,
    manifest: Manifest,
    report_path: Path,
    ticket_column: str,
    batch_size: int,
) -> dict[str, int]:
    data_sequence = {
        entry.original_path: entry.sequence_number
        for entry in manifest.data_files
    }
    counts = _empty_counts()

    try:
        report = open(report_path, "w", encoding="utf-8", newline="")
    except OSError as exc:
        raise CheckError(f"无法写入差异报告 CSV：{exc}") from None

    try:
        with report:
            writer = csv.writer(report)
            writer.writerow(REPORT_HEADER)

            # Dedicated lookup cursor: reusing the streaming cursor for inner
            # queries would reset its result set and silently skip rows.
            lookup = connection.cursor()

            # Pass 1: every billing row -> 错引 / 误计 / 重计.
            cursor = connection.cursor()
            cursor.execute(
                "SELECT row_no, ticket_no, file_path, pos "
                "FROM billing ORDER BY row_no"
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row_no, billing_ticket, file_path, pos in rows:
                    source = lookup.execute(
                        "SELECT ticket_no FROM source "
                        "WHERE file_path = ? AND pos = ? LIMIT 1",
                        (file_path, pos),
                    ).fetchone()
                    if source is None:
                        _write_report_row(
                            writer,
                            counts,
                            DIFF_WRONG_REF,
                            billing_row_no=row_no,
                            billing_ticket=billing_ticket,
                            file_path=file_path,
                            pos=pos,
                            source_ticket=None,
                            detail="按原路径和行号找不到源行",
                        )
                        continue

                    source_ticket = source[0]
                    if source_ticket != billing_ticket:
                        _write_report_row(
                            writer,
                            counts,
                            DIFF_WRONG_REF,
                            billing_row_no=row_no,
                            billing_ticket=billing_ticket,
                            file_path=file_path,
                            pos=pos,
                            source_ticket=source_ticket,
                            detail=(
                                "票号不符：源行票号为 "
                                f"{source_ticket}"
                            ),
                        )
                        continue

                    data_seq = data_sequence.get(file_path)
                    deleted = False
                    if data_seq is not None:
                        deleted = lookup.execute(
                            "SELECT 1 FROM pos_delete "
                            "WHERE file_path = ? AND pos = ? AND seq >= ? "
                            "LIMIT 1",
                            (file_path, pos, data_seq),
                        ).fetchone()
                    if deleted:
                        _write_report_row(
                            writer,
                            counts,
                            DIFF_MISCHARGE,
                            billing_row_no=row_no,
                            billing_ticket=billing_ticket,
                            file_path=file_path,
                            pos=pos,
                            source_ticket=source_ticket,
                            detail="票号相符但源行已被位置删除",
                        )
                        continue

                    matching = lookup.execute(
                        "SELECT COUNT(*) FROM billing "
                        "WHERE file_path = ? AND pos = ? AND ticket_no = ?",
                        (file_path, pos, source_ticket),
                    ).fetchone()[0]
                    if matching >= 2:
                        _write_report_row(
                            writer,
                            counts,
                            DIFF_RECHARGE,
                            billing_row_no=row_no,
                            billing_ticket=billing_ticket,
                            file_path=file_path,
                            pos=pos,
                            source_ticket=source_ticket,
                            detail=(
                                "同一未删源行有 "
                                f"{matching} 条票号相符的计费行"
                            ),
                        )
            cursor.close()
            report.flush()

            # Pass 2: every source row -> 漏计 (live rows without a match).
            cursor = connection.cursor()
            cursor.execute(
                "SELECT file_path, pos, ticket_no FROM source "
                "ORDER BY file_path, pos"
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for file_path, pos, source_ticket in rows:
                    data_seq = data_sequence.get(file_path)
                    if data_seq is not None:
                        deleted = lookup.execute(
                            "SELECT 1 FROM pos_delete "
                            "WHERE file_path = ? AND pos = ? AND seq >= ? "
                            "LIMIT 1",
                            (file_path, pos, data_seq),
                        ).fetchone()
                        if deleted:
                            continue
                    matching = lookup.execute(
                        "SELECT COUNT(*) FROM billing "
                        "WHERE file_path = ? AND pos = ? AND ticket_no = ?",
                        (file_path, pos, source_ticket),
                    ).fetchone()[0]
                    if matching == 0:
                        _write_report_row(
                            writer,
                            counts,
                            DIFF_MISSING,
                            billing_row_no=None,
                            billing_ticket=None,
                            file_path=file_path,
                            pos=pos,
                            source_ticket=source_ticket,
                            detail="未删源行没有票号相符的计费行",
                        )
            cursor.close()
            lookup.close()
            report.flush()
    except CheckError:
        report.close()
        report_path.unlink(missing_ok=True)
        raise
    except (OSError, sqlite3.Error) as exc:
        report.close()
        report_path.unlink(missing_ok=True)
        raise CheckError(f"差异报告读写失败：{exc}") from None

    return counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_reconcile(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    billing_csv: Path,
    report_path: Path,
    ticket_column: str = "ticket_no",
    billing_label: str = "billing.csv",
    *,
    spool_path: Path | None = None,
    batch_size: int = BATCH_SIZE,
) -> ReconcileResult:
    """Run the all-or-nothing whole-day reconciliation.

    ``billing_csv`` is the uploaded billing file; ``report_path`` is where the
    difference CSV is streamed to.  ``spool_path`` defaults to a sibling
    temporary SQLite file removed before returning.  On any failure the
    partial report file is deleted and a :class:`CheckError` is raised.
    """
    if not ticket_column:
        raise CheckError("票号列名不能为空。")
    if not manifest.data_files:
        raise CheckError(
            "清单中没有 data 类型的数据文件，无法进行整日核验。"
        )
    if manifest.equality_delete_files:
        raise CheckError(
            "清单包含等值删除（equality delete）文件。整日核验只接受位置"
            "删除规则，等值删除无法据此核对，本次不生成差异报告。",
            files=[
                entry.uploaded_filename
                for entry in manifest.equality_delete_files
            ],
        )

    _check_presence(manifest, uploaded_paths)

    if spool_path is None:
        spool_path = report_path.parent / f".{report_path.name}.spool.sqlite"
    owns_spool = True
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_spool(spool_path)
        source_rows = _spool_data_files(
            manifest, uploaded_paths, connection, ticket_column
        )
        _spool_position_deletes(manifest, uploaded_paths, connection)
        billing_rows = _spool_billing_csv(
            billing_csv, billing_label, connection
        )

        connection.executescript(
            """
            CREATE INDEX idx_source_lookup ON source(file_path, pos);
            CREATE INDEX idx_billing_lookup
                ON billing(file_path, pos, ticket_no);
            CREATE INDEX idx_delete_lookup ON pos_delete(file_path, pos);
            """
        )
        connection.commit()

        counts = _classify(
            connection,
            manifest,
            report_path,
            ticket_column,
            batch_size,
        )
    except CheckError:
        report_path.unlink(missing_ok=True)
        raise
    finally:
        if connection is not None:
            connection.close()
        if owns_spool:
            try:
                Path(spool_path).unlink(missing_ok=True)
            except OSError:
                pass

    referenced = {entry.uploaded_filename for entry in manifest.entries}
    unused_uploads = sorted(
        name for name in uploaded_paths if name not in referenced
    )
    warnings: list[str] = []
    if unused_uploads:
        warnings.append(
            "以下上传文件未出现在清单中，已忽略："
            + "、".join(unused_uploads)
        )

    return ReconcileResult(
        manifest=manifest,
        ticket_column=ticket_column,
        billing_row_count=billing_rows,
        source_row_count=source_rows,
        diff_counts=counts,
        report_path=report_path,
        warnings=warnings,
    )
