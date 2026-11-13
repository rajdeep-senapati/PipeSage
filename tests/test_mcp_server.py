
import json

from src.mcp_server import server


def test_list_failure_scenarios():
    scenarios = server.list_failure_scenarios()

    assert "clean" in scenarios
    assert "negative_price" in scenarios
    assert len(scenarios) == 7


def test_get_quality_report(tmp_path, monkeypatch):
    report = {
        "timestamp": "2026-11-13T10:00:00+00:00",
        "overall_status": "passed",
        "total_records": 10,
        "accepted_records": 10,
        "rejected_records": 0,
        "rejection_rate": 0.0,
        "total_rules": 10,
        "passed_rule_count": 10,
        "failed_rule_count": 0,
        "pipeline_status": "completed",
    }

    monkeypatch.setattr(server, "OUTPUT_DIR", tmp_path)

    report_path = tmp_path / "clean_quality_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    result = server.get_quality_report("clean")

    assert result["overall_status"] == "passed"
    assert result["total_records"] == 10


def test_get_pipeline_metrics(tmp_path, monkeypatch):
    report = {
        "timestamp": "2026-11-13T10:00:00+00:00",
        "pipeline_status": "completed_with_quarantine",
        "overall_status": "failed",
        "total_records": 10,
        "accepted_records": 9,
        "rejected_records": 1,
        "rejection_rate": 0.1,
        "total_rules": 10,
        "passed_rule_count": 9,
        "failed_rule_count": 1,
    }

    monkeypatch.setattr(server, "OUTPUT_DIR", tmp_path)

    report_path = tmp_path / "negative_price_quality_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    result = server.get_pipeline_metrics("negative_price")

    assert result["accepted_records"] == 9
    assert result["rejected_records"] == 1
    assert result["rejection_rate"] == 0.1
    assert result["failed_rule_count"] == 1


def test_get_quality_report_rejects_invalid_scenario():
    import pytest

    with pytest.raises(ValueError):
        server.get_quality_report("unknown_scenario")


def test_get_quality_report_when_missing(tmp_path, monkeypatch):
    import pytest

    monkeypatch.setattr(server, "OUTPUT_DIR", tmp_path)

    with pytest.raises(FileNotFoundError):
        server.get_quality_report("clean")


def test_validation_rules_resource(monkeypatch, tmp_path):
    config = tmp_path / "validation_rules.json"
    config.write_text(
        '{"rules": [{"column": "order_id", "checks": ["not_null"]}]}',
        encoding="utf-8",
    )

    monkeypatch.setattr(server, "VALIDATION_CONFIG", config)

    result = server.validation_rules()

    assert '"order_id"' in result
    assert '"not_null"' in result
