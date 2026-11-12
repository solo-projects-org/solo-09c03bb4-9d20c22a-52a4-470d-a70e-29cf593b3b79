"""Flask front end for the Iceberg v2 reconciliation tools."""

from __future__ import annotations

import os
import re
import secrets
import shutil
import tempfile
import time
from pathlib import Path

from flask import (
    Flask,
    abort,
    render_template,
    request,
    send_from_directory,
)

from checker.engine import CheckResult, run_check
from checker.errors import CheckError
from checker.manifest import parse_manifest
from checker.reconcile import (
    BILLING_COLUMNS,
    ReconcileResult,
    run_reconcile,
)

ROOT = Path(__file__).resolve().parent
SAMPLE_DIR = ROOT / "sample"
SAMPLE_TICKETS = "4242, 1003, 4218, 8888, 9999"
SAMPLE_TICKET_COLUMN = "ticket_no"
SAMPLE_BILLING_CSV = "billing-sample.csv"
SAMPLE_RECONCILE_MANIFEST = "manifest-reconcile.csv"
REPORT_TTL_SECONDS = int(os.environ.get("REPORT_TTL_SECONDS", str(3600)))
REPORT_KEEP_MAX = 100
REPORT_FILE_NAME = "diff-report.csv"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = (
    int(os.environ.get("MAX_UPLOAD_MB", "512")) * 1024 * 1024
)


def _report_root() -> Path:
    root = Path(
        os.environ.get("REPORT_DIR") or (ROOT / "var" / "reconcile-reports")
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def parse_tickets(text: str) -> list[int]:
    tokens = [
        token
        for token in re.split(r"[,\s，、;；]+", (text or "").strip())
        if token
    ]
    if not tokens:
        raise CheckError("请输入至少一个整数票号。")
    tickets: list[int] = []
    for token in tokens:
        if not re.fullmatch(r"[+-]?\d+", token):
            raise CheckError(f"票号 “{token}” 不是整数。")
        tickets.append(int(token))
    return tickets


def _save_uploads(file_storages, temp_dir: Path) -> dict[str, Path]:
    uploaded: dict[str, Path] = {}
    for file_storage in file_storages:
        if file_storage is None or not file_storage.filename:
            continue
        # Browsers occasionally send a client-side path; keep only the name.
        name = os.path.basename(file_storage.filename.replace("\\", "/")).strip()
        if not name:
            raise CheckError("上传的 Parquet 文件名为空。")
        if name in uploaded:
            raise CheckError(
                f"上传文件名重复：{name}。清单中的 uploaded_filename 必须唯一。",
                files=[name],
            )
        destination = temp_dir / f"upload-{len(uploaded) + 1:04d}.parquet"
        try:
            file_storage.save(destination)
        except Exception as exc:
            raise CheckError(
                f"无法保存上传文件 “{name}”：{exc}", files=[name]
            ) from None
        if destination.stat().st_size == 0:
            raise CheckError(f"上传文件 “{name}” 为空。", files=[name])
        uploaded[name] = destination
    return uploaded


def _sample_uploaded_paths(manifest) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for entry in manifest.required_entries:
        candidate = SAMPLE_DIR / entry.uploaded_filename
        if candidate.is_file():
            paths[entry.uploaded_filename] = candidate
    return paths


def _sweep_reports(root: Path) -> None:
    """Remove expired report directories, then cap the remaining count."""
    now = time.time()
    entries = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        report = child / REPORT_FILE_NAME
        try:
            age = now - report.stat().st_mtime
        except OSError:
            continue
        if age > REPORT_TTL_SECONDS:
            _remove_report_dir(child)
            continue
        entries.append((report.stat().st_mtime, child))
    entries.sort(key=lambda item: item[0], reverse=True)
    for _age, child in entries[REPORT_KEEP_MAX:]:
        _remove_report_dir(child)


def _remove_report_dir(directory: Path) -> None:
    for path in sorted(directory.glob("**/*"), reverse=True):
        try:
            path.unlink()
        except OSError:
            pass
    try:
        directory.rmdir()
    except OSError:
        pass


def _store_report(result: ReconcileResult) -> str:
    root = _report_root()
    _sweep_reports(root)
    token = secrets.token_hex(16)
    target_dir = root / token
    target_dir.mkdir(parents=False, exist_ok=False)
    target = target_dir / REPORT_FILE_NAME
    try:
        # shutil.move falls back to copy+unlink across filesystems.
        shutil.move(str(result.report_path), str(target))
    except OSError as exc:
        _remove_report_dir(target_dir)
        raise CheckError(f"无法保存差异报告：{exc}") from None
    result.report_path = target
    result.report_token = token
    return token


def _render(
    *,
    result: CheckResult | None = None,
    reconcile: ReconcileResult | None = None,
    error: CheckError | None = None,
    tickets_text: str = "",
    ticket_column: str = "ticket_no",
    source: str = "",
    status: int = 200,
):
    return (
        render_template(
            "index.html",
            result=result,
            reconcile=reconcile,
            error=error,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source=source,
            sample_tickets=SAMPLE_TICKETS,
            billing_columns="、".join(BILLING_COLUMNS),
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
        ),
        status,
    )


@app.get("/")
def index():
    return _render()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/sample")
def sample():
    try:
        manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
        result = run_check(
            manifest,
            _sample_uploaded_paths(manifest),
            parse_tickets(SAMPLE_TICKETS),
            SAMPLE_TICKET_COLUMN,
        )
    except CheckError as exc:
        return _render(
            error=exc,
            tickets_text=SAMPLE_TICKETS,
            ticket_column=SAMPLE_TICKET_COLUMN,
            source="sample",
            status=400,
        )
    return _render(
        result=result,
        tickets_text=SAMPLE_TICKETS,
        ticket_column=SAMPLE_TICKET_COLUMN,
        source="sample",
    )


@app.post("/check")
def check():
    tickets_text = request.form.get("tickets", "")
    ticket_column = request.form.get("ticket_column", "ticket_no").strip()
    manifest_file = request.files.get("manifest")
    parquet_files = request.files.getlist("files")

    if manifest_file is None or not manifest_file.filename:
        return _render(
            error=CheckError("请上传 Iceberg v2 有效文件清单（CSV 或 JSON）。"),
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )

    try:
        tickets = parse_tickets(tickets_text)
        manifest = parse_manifest(manifest_file.read())
        with tempfile.TemporaryDirectory(prefix="iceberg-pos-delete-") as temp:
            uploaded_paths = _save_uploads(parquet_files, Path(temp))
            result = run_check(
                manifest, uploaded_paths, tickets, ticket_column
            )
    except CheckError as exc:
        return _render(
            error=exc,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )

    return _render(
        result=result,
        tickets_text=tickets_text,
        ticket_column=ticket_column,
        source="upload",
    )


def _run_reconcile_request(
    *,
    manifest_file,
    parquet_files,
    billing_file,
    ticket_column: str,
    source: str,
    sample_mode: bool = False,
):
    try:
        if sample_mode:
            manifest = parse_manifest(
                (SAMPLE_DIR / SAMPLE_RECONCILE_MANIFEST).read_bytes()
            )
            with tempfile.TemporaryDirectory(prefix="reconcile-") as temp:
                temp_path = Path(temp)
                billing_path = temp_path / SAMPLE_BILLING_CSV
                billing_path.write_bytes(
                    (SAMPLE_DIR / SAMPLE_BILLING_CSV).read_bytes()
                )
                report_path = temp_path / REPORT_FILE_NAME
                reconcile_result = run_reconcile(
                    manifest,
                    _sample_uploaded_paths(manifest),
                    billing_path,
                    report_path,
                    ticket_column=ticket_column,
                    billing_label=SAMPLE_BILLING_CSV,
                )
                _store_report(reconcile_result)
        else:
            if manifest_file is None or not manifest_file.filename:
                raise CheckError(
                    "请上传 Iceberg v2 有效文件清单（CSV 或 JSON）。"
                )
            if billing_file is None or not billing_file.filename:
                raise CheckError(
                    "请上传整日计费 CSV（包含 ticket_no、file_path、pos 列）。"
                )
            manifest = parse_manifest(manifest_file.read())
            with tempfile.TemporaryDirectory(prefix="reconcile-") as temp:
                temp_path = Path(temp)
                uploaded_paths = _save_uploads(parquet_files, temp_path)
                billing_name = os.path.basename(
                    billing_file.filename.replace("\\", "/")
                ).strip() or "billing.csv"
                billing_path = temp_path / "billing-upload.csv"
                try:
                    billing_file.save(billing_path)
                except Exception as exc:
                    raise CheckError(
                        f"无法保存计费 CSV “{billing_name}”：{exc}",
                        files=[billing_name],
                    ) from None
                if billing_path.stat().st_size == 0:
                    raise CheckError(
                        f"计费 CSV “{billing_name}” 为空。",
                        files=[billing_name],
                    )
                report_path = temp_path / REPORT_FILE_NAME
                reconcile_result = run_reconcile(
                    manifest,
                    uploaded_paths,
                    billing_path,
                    report_path,
                    ticket_column=ticket_column,
                    billing_label=billing_name,
                )
                _store_report(reconcile_result)
    except CheckError as exc:
        return _render(
            error=exc,
            ticket_column=ticket_column,
            source=source,
            status=400,
        )

    return _render(reconcile=reconcile_result, ticket_column=ticket_column, source=source)


@app.get("/reconcile/sample")
def reconcile_sample():
    return _run_reconcile_request(
        manifest_file=None,
        parquet_files=None,
        billing_file=None,
        ticket_column=SAMPLE_TICKET_COLUMN,
        source="reconcile-sample",
        sample_mode=True,
    )


@app.post("/reconcile/check")
def reconcile_check():
    ticket_column = request.form.get("ticket_column", "ticket_no").strip()
    return _run_reconcile_request(
        manifest_file=request.files.get("manifest"),
        parquet_files=request.files.getlist("files"),
        billing_file=request.files.get("billing"),
        ticket_column=ticket_column,
        source="reconcile-upload",
    )


@app.get("/reconcile/report/<token>")
def reconcile_report(token: str):
    if not re.fullmatch(r"[0-9a-f]{32}", token):
        abort(404)
    report_dir = _report_root() / token
    report = report_dir / REPORT_FILE_NAME
    if not report.is_file():
        abort(404)
    if time.time() - report.stat().st_mtime > REPORT_TTL_SECONDS:
        _remove_report_dir(report_dir)
        abort(404)
    return send_from_directory(
        report_dir,
        REPORT_FILE_NAME,
        as_attachment=True,
        download_name="reconcile-diff-report.csv",
        mimetype="text/csv",
    )


@app.errorhandler(404)
def not_found(_error):
    return _render(
        error=CheckError("请求的差异报告不存在或已过期。"),
        status=404,
    )


@app.errorhandler(413)
def payload_too_large(_error):
    return _render(
        error=CheckError(
            "上传内容超过大小限制 "
            f"（{app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB）。"
        ),
        status=413,
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        debug=bool(os.environ.get("FLASK_DEBUG")),
    )
