from __future__ import annotations

import io
import re
from pathlib import Path

import pytest

from app import REPORT_TTL_SECONDS, app

SAMPLE = Path(__file__).resolve().parents[1] / "sample"

RECONCILE_PARQUETS = (
    "data-0001.parquet",
    "data-0002.parquet",
    "delete-pos-0001.parquet",
    "delete-pos-0002.parquet",
    "delete-pos-stale-0001.parquet",
)


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def _sample_form(client, *, manifest_name="manifest-reconcile.csv", billing="billing-sample.csv"):
    files = []
    for name in RECONCILE_PARQUETS:
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    return client.post(
        "/reconcile/check",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / manifest_name).read_bytes()),
                manifest_name,
            ),
            "billing": (
                io.BytesIO((SAMPLE / billing).read_bytes()),
                billing,
            ),
            "files": files,
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )


def test_reconcile_sample_page_lists_all_four_categories_and_download(client):
    response = client.get("/reconcile/sample")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "整日核验结果" in html
    assert "内置样例" in html
    for label in ("错引", "误计", "重计", "漏计"):
        assert label in html
    match = re.search(r"/reconcile/report/([0-9a-f]{32})", html)
    assert match is not None

    download = client.get(match.group(0))
    assert download.status_code == 200
    assert download.mimetype == "text/csv"
    assert "attachment" in download.headers["Content-Disposition"]
    text = download.data.decode("utf-8")
    lines = text.splitlines()
    assert lines[0] == (
        "diff_type,billing_row_no,billing_ticket_no,file_path,pos,"
        "source_ticket_no,detail"
    )
    diff_types = {line.split(",", 1)[0] for line in lines[1:]}
    assert diff_types == {"错引", "误计", "重计", "漏计"}


def test_reconcile_sample_contains_recharge_and_missing(client):
    response = client.get("/reconcile/sample")
    html = response.get_data(as_text=True)
    # The sample is explicitly required to have both recharges and missing.
    assert "重计" in html
    assert "漏计" in html


def test_reconcile_upload_round_trip(client):
    response = _sample_form(client)
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "整日核验结果" in html
    assert "下载差异 CSV" in html
    match = re.search(r"/reconcile/report/([0-9a-f]{32})", html)
    assert match is not None
    download = client.get(match.group(0))
    assert download.status_code == 200
    assert b"diff_type" in download.data


def test_reconcile_equality_delete_manifest_is_rejected(client):
    response = _sample_form(client, manifest_name="manifest.csv")
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "等值删除" in html
    assert "整日核验结果" not in html


def test_reconcile_missing_billing_is_rejected(client):
    files = [
        (
            io.BytesIO((SAMPLE / name).read_bytes()),
            name,
            "application/octet-stream",
        )
        for name in RECONCILE_PARQUETS
    ]
    response = client.post(
        "/reconcile/check",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest-reconcile.csv").read_bytes()),
                "manifest-reconcile.csv",
            ),
            "files": files,
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "计费 CSV" in response.get_data(as_text=True)


def test_reconcile_bad_billing_row_is_rejected_and_no_report(client):
    bad_billing = (
        "ticket_no,file_path,pos\n"
        "not-a-ticket,s3://bucket/data/data-0001.parquet,0\n"
    )
    files = [
        (
            io.BytesIO((SAMPLE / name).read_bytes()),
            name,
            "application/octet-stream",
        )
        for name in RECONCILE_PARQUETS
    ]
    response = client.post(
        "/reconcile/check",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest-reconcile.csv").read_bytes()),
                "manifest-reconcile.csv",
            ),
            "billing": (io.BytesIO(bad_billing.encode()), "billing.csv"),
            "files": files,
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "坏行" in html
    assert "整日核验结果" not in html


def test_reconcile_missing_replica_is_rejected(client):
    files = [
        (
            io.BytesIO((SAMPLE / name).read_bytes()),
            name,
            "application/octet-stream",
        )
        for name in RECONCILE_PARQUETS
        if name != "data-0002.parquet"
    ]
    response = client.post(
        "/reconcile/check",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest-reconcile.csv").read_bytes()),
                "manifest-reconcile.csv",
            ),
            "billing": (
                io.BytesIO((SAMPLE / "billing-sample.csv").read_bytes()),
                "billing-sample.csv",
            ),
            "files": files,
            "ticket_column": "ticket_no",
        },
        content_type="multipart/form-data",
    )
    html = response.get_data(as_text=True)
    assert response.status_code == 400
    assert "data-0002.parquet" in html
    assert "整日核验结果" not in html


def test_report_unknown_token_returns_404(client):
    response = client.get("/reconcile/report/" + "a" * 32)
    assert response.status_code == 404


def test_report_rejects_malformed_token(client):
    response = client.get("/reconcile/report/../etc")
    assert response.status_code == 404


def test_report_ttl_constant_is_positive():
    assert REPORT_TTL_SECONDS > 0
