from __future__ import annotations

import csv
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.reconcile import (
    BATCH_SIZE,
    DIFF_MISCHARGE,
    DIFF_MISSING,
    DIFF_RECHARGE,
    DIFF_WRONG_REF,
    REPORT_HEADER,
    run_reconcile,
)

DATA_SCHEMA = pa.schema(
    [
        ("ticket_no", pa.int64()),
        ("passenger_name", pa.string()),
    ]
)
DELETE_SCHEMA = pa.schema(
    [
        ("file_path", pa.string()),
        ("pos", pa.int64()),
    ]
)


def _write_data(path: Path, tickets: list[int]) -> None:
    rows = [{"ticket_no": t, "passenger_name": f"P-{t}"} for t in tickets]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=DATA_SCHEMA),
        path,
        row_group_size=4,
    )


def _write_delete(path: Path, records: list[tuple[str, int]]) -> None:
    rows = [{"file_path": p, "pos": pos} for p, pos in records]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=DELETE_SCHEMA), path
    )


def _write_billing(path: Path, rows: list[tuple[int, str, int]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ticket_no", "file_path", "pos"])
        writer.writerows(rows)


def _read_report(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture()
def workspace(tmp_path: Path):
    data_path = tmp_path / "data-0001.parquet"
    delete_path = tmp_path / "delete-0001.parquet"
    manifest_path = tmp_path / "manifest.csv"
    billing_path = tmp_path / "billing.csv"
    report_path = tmp_path / "report.csv"
    source = "s3://bucket/voyage/data/data-0001.parquet"
    delete_source = "s3://bucket/voyage/delete/delete-0001.parquet"

    _write_data(data_path, [10, 20, 30, 40, 50, 60])
    _write_delete(delete_path, [(source, 1)])  # deletes ticket 20
    manifest_path.write_text(
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{source},data-0001.parquet,data,7\n"
        f"{delete_source},delete-0001.parquet,position_delete,9\n",
        encoding="utf-8",
    )
    manifest = parse_manifest(manifest_path.read_bytes())
    uploads = {
        "data-0001.parquet": data_path,
        "delete-0001.parquet": delete_path,
    }
    return {
        "tmp": tmp_path,
        "manifest": manifest,
        "uploads": uploads,
        "billing": billing_path,
        "report": report_path,
        "source": source,
    }


def _run(workspace, rows, **kwargs):
    _write_billing(workspace["billing"], rows)
    return run_reconcile(
        workspace["manifest"],
        workspace["uploads"],
        workspace["billing"],
        workspace["report"],
        billing_label="billing.csv",
        **kwargs,
    )


def _by_type(report_rows, diff_type):
    return [row for row in report_rows if row["diff_type"] == diff_type]


def test_clean_billing_has_no_differences(workspace):
    source = workspace["source"]
    # Row 1 (ticket 20) is position-deleted: a clean billing file simply
    # never references it.
    rows = [
        (10, source, 0),
        (30, source, 2),
        (40, source, 3),
        (50, source, 4),
        (60, source, 5),
    ]
    result = _run(workspace, rows)
    assert result.source_row_count == 6
    assert result.billing_row_count == 5
    assert result.total_diffs == 0
    report = _read_report(workspace["report"])
    assert len(report) == 0
    with workspace["report"].open(encoding="utf-8") as handle:
        assert handle.readline().strip().split(",") == list(REPORT_HEADER)


def test_mischarge_when_source_row_is_deleted(workspace):
    source = workspace["source"]
    rows = [(t, source, pos) for pos, t in enumerate([10, 20, 30, 40, 50, 60])]
    result = _run(workspace, rows)
    assert result.diff_counts[DIFF_MISCHARGE] == 1
    report = _read_report(workspace["report"])
    mischarge = _by_type(report, DIFF_MISCHARGE)
    assert len(mischarge) == 1
    # Billing data row numbers are 1-based.
    assert mischarge[0]["billing_row_no"] == "2"
    assert mischarge[0]["billing_ticket_no"] == "20"
    assert mischarge[0]["pos"] == "1"
    assert mischarge[0]["source_ticket_no"] == "20"


def test_wrong_reference_for_unknown_position_and_path(workspace):
    source = workspace["source"]
    rows = [
        (10, source, 0),
        (30, source, 2),
        (40, source, 3),
        (50, source, 4),
        (60, source, 5),
        (70, source, 99),  # pos out of range
        (80, "s3://bucket/other.parquet", 0),  # unknown path
    ]
    result = _run(workspace, rows)
    report = _read_report(workspace["report"])
    wrong = _by_type(report, DIFF_WRONG_REF)
    assert {(r["billing_row_no"], r["billing_ticket_no"]) for r in wrong} == {
        ("6", "70"),
        ("7", "80"),
    }
    assert all(r["source_ticket_no"] == "" for r in wrong)
    # Live row 20 is deleted so it is not a missing charge; the deleted row is
    # simply absent from billing.
    assert result.diff_counts[DIFF_MISSING] == 0


def test_wrong_reference_ticket_mismatch_also_marks_source_missing(workspace):
    source = workspace["source"]
    # Row 0 (ticket 10) is billed as ticket 99 -> 错引, and the live row has
    # no ticket-matching billing row -> 漏计.
    rows = [
        (99, source, 0),
        (20, source, 1),
        (30, source, 2),
        (40, source, 3),
        (50, source, 4),
        (60, source, 5),
    ]
    result = _run(workspace, rows)
    report = _read_report(workspace["report"])
    wrong = _by_type(report, DIFF_WRONG_REF)
    assert len(wrong) == 1
    assert wrong[0]["billing_row_no"] == "1"
    assert wrong[0]["source_ticket_no"] == "10"
    missing = _by_type(report, DIFF_MISSING)
    assert len(missing) == 1
    assert missing[0]["file_path"] == source
    assert missing[0]["pos"] == "0"
    assert missing[0]["source_ticket_no"] == "10"
    assert missing[0]["billing_row_no"] == ""


def test_recharge_reports_every_duplicate_billing_row(workspace):
    source = workspace["source"]
    rows = [
        (10, source, 0),
        (20, source, 1),
        (30, source, 2),
        (30, source, 2),  # duplicate of live row 2
        (30, source, 2),  # third copy -> still all three reported
        (40, source, 3),
        (50, source, 4),
        (60, source, 5),
    ]
    result = _run(workspace, rows)
    report = _read_report(workspace["report"])
    recharge = _by_type(report, DIFF_RECHARGE)
    assert len(recharge) == 3
    assert [r["billing_row_no"] for r in recharge] == ["3", "4", "5"]
    assert result.diff_counts[DIFF_RECHARGE] == 3
    # A recharged live row is not also a missing charge.
    assert result.diff_counts[DIFF_MISSING] == 0


def test_missing_charge_for_live_row_without_billing(workspace):
    source = workspace["source"]
    # Live rows 3 (ticket 40) and 4 (ticket 50) omitted entirely.
    rows = [
        (10, source, 0),
        (20, source, 1),
        (30, source, 2),
        (60, source, 5),
    ]
    result = _run(workspace, rows)
    report = _read_report(workspace["report"])
    missing = _by_type(report, DIFF_MISSING)
    assert {(r["pos"], r["source_ticket_no"]) for r in missing} == {
        ("3", "40"),
        ("4", "50"),
    }
    assert all(r["billing_row_no"] == "" for r in missing)


def test_wrong_reference_takes_precedence_over_mischarge(workspace):
    source = workspace["source"]
    # Deleted row 1 (ticket 20) billed with wrong ticket 21 -> 错引, not 误计.
    rows = [
        (10, source, 0),
        (21, source, 1),
        (30, source, 2),
        (40, source, 3),
        (50, source, 4),
        (60, source, 5),
    ]
    result = _run(workspace, rows)
    report = _read_report(workspace["report"])
    assert _by_type(report, DIFF_WRONG_REF)
    assert not _by_type(report, DIFF_MISCHARGE)
    # Deleted rows never become missing charges.
    assert not _by_type(report, DIFF_MISSING)


def test_stale_sequence_delete_does_not_count_as_deleted(workspace, tmp_path):
    source = workspace["source"]
    stale = tmp_path / "stale-delete.parquet"
    _write_delete(stale, [(source, 2)])  # targets ticket 30
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{source},data-0001.parquet,data,7\n"
        f"{workspace['tmp'] / 'delete-0001.parquet'},"
        "delete-0001.parquet,position_delete,9\n"
        f"s3://bucket/voyage/delete/stale.parquet,stale-delete.parquet,"
        "position_delete,6\n"
    )
    manifest = parse_manifest(manifest_text)
    uploads = dict(workspace["uploads"])
    uploads["stale-delete.parquet"] = stale
    billing = tmp_path / "billing.csv"
    _write_billing(
        billing,
        [(t, source, pos) for pos, t in enumerate([10, 20, 30, 40, 50, 60])],
    )
    result = run_reconcile(
        manifest,
        uploads,
        billing,
        tmp_path / "report.csv",
    )
    # Ticket 20 still mischarged by the in-scope delete; ticket 30 stays live.
    assert result.diff_counts[DIFF_MISCHARGE] == 1
    assert result.diff_counts[DIFF_MISSING] == 0


def test_equality_delete_aborts_without_report(workspace, tmp_path):
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{workspace['source']},data-0001.parquet,data,7\n"
        "s3://bucket/voyage/delete-eq/eq.parquet,eq.parquet,"
        "equality_delete,9\n"
    )
    manifest = parse_manifest(manifest_text)
    uploads = {"data-0001.parquet": workspace["uploads"]["data-0001.parquet"]}
    billing = tmp_path / "billing.csv"
    _write_billing(billing, [(10, workspace["source"], 0)])
    report = tmp_path / "report.csv"
    with pytest.raises(CheckError, match="等值删除"):
        run_reconcile(manifest, uploads, billing, report)
    assert not report.exists()


def test_missing_replica_aborts(workspace):
    uploads = dict(workspace["uploads"])
    del uploads["delete-0001.parquet"]
    _write_billing(
        workspace["billing"], [(10, workspace["source"], 0)]
    )
    with pytest.raises(CheckError) as excinfo:
        run_reconcile(
            workspace["manifest"],
            uploads,
            workspace["billing"],
            workspace["report"],
        )
    assert "delete-0001.parquet" in excinfo.value.files
    assert not workspace["report"].exists()


def test_corrupt_parquet_aborts_with_file_name(workspace):
    bad = workspace["tmp"] / "data-0001.parquet"
    bad.write_bytes(b"not parquet at all")
    _write_billing(
        workspace["billing"], [(10, workspace["source"], 0)]
    )
    with pytest.raises(CheckError) as excinfo:
        run_reconcile(
            workspace["manifest"],
            workspace["uploads"],
            workspace["billing"],
            workspace["report"],
        )
    assert "data-0001.parquet" in excinfo.value.files


def test_bad_billing_rows_abort(workspace):
    cases = [
        "ticket_no,file_path,pos\n10,,0\n",  # empty path
        "ticket_no,file_path,pos\n,s3://b/0,0\n",  # empty ticket
        "ticket_no,file_path,pos\n10,s3://b/0,\n",  # empty pos
        "ticket_no,file_path,pos\nnope,s3://b/0,0\n",  # non-integer ticket
        "ticket_no,file_path,pos\n10,s3://b/0,-3\n",  # negative pos
        "ticket,file_path,pos\n10,s3://b/0,0\n",  # missing header column
    ]
    for index, content in enumerate(cases):
        billing = workspace["tmp"] / f"bad-{index}.csv"
        billing.write_text(content, encoding="utf-8")
        report = workspace["tmp"] / f"report-{index}.csv"
        with pytest.raises(CheckError):
            run_reconcile(
                workspace["manifest"],
                workspace["uploads"],
                billing,
                report,
                billing_label=billing.name,
            )
        assert not report.exists()


def test_billing_data_row_numbers_ignore_blank_lines(workspace):
    source = workspace["source"]
    billing = workspace["billing"]
    billing.write_text(
        "ticket_no,file_path,pos\n"
        "\n"
        f"10,{source},0\n"
        "\n"
        f"99,{source},99\n",  # unresolved wrong reference, data row 2
        encoding="utf-8",
    )
    result = run_reconcile(
        workspace["manifest"],
        workspace["uploads"],
        billing,
        workspace["report"],
    )
    report = _read_report(workspace["report"])
    wrong = _by_type(report, DIFF_WRONG_REF)
    assert len(wrong) == 1
    assert wrong[0]["billing_row_no"] == "2"
    assert result.billing_row_count == 2


def test_empty_billing_marks_every_live_row_missing(workspace):
    billing = workspace["billing"]
    billing.write_text("ticket_no,file_path,pos\n", encoding="utf-8")
    result = run_reconcile(
        workspace["manifest"],
        workspace["uploads"],
        billing,
        workspace["report"],
    )
    report = _read_report(workspace["report"])
    # Five live rows (one of the six source rows is position-deleted).
    missing = _by_type(report, DIFF_MISSING)
    assert len(missing) == 5
    assert result.billing_row_count == 0


def test_spool_sqlite_is_removed_after_run(workspace):
    _write_billing(
        workspace["billing"], [(10, workspace["source"], 0)]
    )
    run_reconcile(
        workspace["manifest"],
        workspace["uploads"],
        workspace["billing"],
        workspace["report"],
    )
    leftovers = list(workspace["tmp"].glob("*.sqlite"))
    assert leftovers == []


def test_batches_smaller_than_data_stay_correct(workspace):
    source = workspace["source"]
    rows = [(t, source, pos) for pos, t in enumerate([10, 20, 30, 40, 50, 60])]
    result = _run(workspace, rows, batch_size=2)
    # Same single mischarge regardless of batch size.
    assert result.diff_counts == {
        DIFF_WRONG_REF: 0,
        DIFF_MISCHARGE: 1,
        DIFF_RECHARGE: 0,
        DIFF_MISSING: 0,
    }


def test_batch_size_one_processes_every_row_without_skipping(workspace):
    """Regression: inner queries must not reset the streaming cursor."""
    source = workspace["source"]
    # Rows 3 and 4 omitted (missing charges); deleted row 1 billed (mischarge);
    # one unresolved reference. Every row must be visited when fetchmany
    # returns a single row at a time.
    rows = [
        (10, source, 0),
        (20, source, 1),
        (30, source, 2),
        (60, source, 5),
        (70, source, 99),
    ]
    result = _run(workspace, rows, batch_size=1)
    assert result.billing_row_count == 5
    assert result.diff_counts[DIFF_MISCHARGE] == 1
    assert result.diff_counts[DIFF_WRONG_REF] == 1
    report = _read_report(workspace["report"])
    visited = {
        row["billing_row_no"]
        for row in report
        if row["billing_row_no"]
    }
    assert "5" in visited
    # Pass 2 still walks every source row even with batch size one.
    missing = _by_type(report, DIFF_MISSING)
    assert {(row["pos"], row["source_ticket_no"]) for row in missing} == {
        ("3", "40"),
        ("4", "50"),
    }


def test_batch_size_constant_is_bounded():
    # The streaming design relies on a fixed, modest batch size.
    assert BATCH_SIZE <= 10_000


def test_duplicate_billing_header_is_rejected(workspace):
    billing = workspace["billing"]
    billing.write_text(
        "ticket_no,ticket_no,file_path,pos\n"
        f"10,10,{workspace['source']},0\n",
        encoding="utf-8",
    )
    with pytest.raises(CheckError, match="重复列名"):
        run_reconcile(
            workspace["manifest"],
            workspace["uploads"],
            billing,
            workspace["report"],
        )
    assert not workspace["report"].exists()


def test_extra_billing_columns_are_ignored(workspace):
    source = workspace["source"]
    billing = workspace["billing"]
    billing.write_text(
        "ticket_no,file_path,pos,note\n"
        f"10,{source},0,ok\n"
        f"30,{source},2,ok\n"
        f"40,{source},3,ok\n"
        f"50,{source},4,ok\n"
        f"60,{source},5,ok\n",
        encoding="utf-8",
    )
    result = run_reconcile(
        workspace["manifest"],
        workspace["uploads"],
        billing,
        workspace["report"],
    )
    assert result.billing_row_count == 5
    assert result.total_diffs == 0
