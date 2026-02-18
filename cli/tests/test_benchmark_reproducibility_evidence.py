from __future__ import annotations

from tests.scripts.benchmark_reproducibility import run_reproducibility_check


def test_benchmark_reproducibility_evidence_gate() -> None:
    result = run_reproducibility_check()
    artifact = result.artifact_path

    assert artifact.exists()
    assert artifact.is_file()
    assert result.fixture_path == "tests/fixtures/large.pdf"
    assert len(result.rss_kb_runs) == 3
    assert result.rss_delta_percent <= result.gate_threshold_percent
    assert result.gate_threshold_percent == 15.0
    assert result.gate_passed
    assert len(result.total_seconds_runs) == 3
    assert result.total_seconds_gate_threshold_percent == 20.0
    assert result.total_seconds_gate_mode in {"percent", "absolute"}
    assert result.total_seconds_delta_abs_seconds >= 0.0
    assert result.total_seconds_gate_passed
    if result.total_seconds_gate_mode == "percent":
        assert result.total_seconds_delta_percent <= result.total_seconds_gate_threshold_percent
    else:
        assert result.total_seconds_delta_abs_seconds <= 0.150
    assert result.total_seconds_p50 > 0.0
    assert result.total_seconds_p95 >= result.total_seconds_p50

    artifact_text = artifact.read_text(encoding="utf-8")
    assert "fixture_path: tests/fixtures/large.pdf" in artifact_text
    assert "fixture_sha256:" in artifact_text
    assert "fixture_bytes:" in artifact_text
    assert "run_1_rss_kb:" in artifact_text
    assert "run_2_rss_kb:" in artifact_text
    assert "run_3_rss_kb:" in artifact_text
    assert "delta_formula_percent: ((max_rss_kb-min_rss_kb)/max_rss_kb)*100" in artifact_text
    assert "rss_delta_percent:" in artifact_text
    assert "reproducibility_gate: PASS" in artifact_text
    assert "run_1_total_seconds:" in artifact_text
    assert "run_2_total_seconds:" in artifact_text
    assert "run_3_total_seconds:" in artifact_text
    assert "total_seconds_delta_percent:" in artifact_text
    assert "total_seconds_delta_abs_seconds:" in artifact_text
    assert "total_seconds_gate_mode:" in artifact_text
    assert "total_seconds_reproducibility_gate: PASS" in artifact_text
    assert "total_seconds_p50:" in artifact_text
    assert "total_seconds_p95:" in artifact_text
