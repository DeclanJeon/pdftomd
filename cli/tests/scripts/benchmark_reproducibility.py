from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast


class FixtureContract(TypedDict):
    path: str
    sha256: str
    bytes: int


class ContractFile(TypedDict):
    canonical_benchmark_fixture: FixtureContract


@dataclass(frozen=True)
class BenchmarkRun:
    index: int
    rss_kb: int
    stage_native_seconds: float
    stage_ocr_seconds: float
    stage_postprocess_seconds: float
    stage_write_seconds: float
    stage_total_seconds: float
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ReproducibilityResult:
    artifact_path: Path
    fixture_path: str
    fixture_sha256: str
    fixture_bytes: int
    rss_kb_runs: tuple[int, int, int]
    rss_delta_percent: float
    gate_threshold_percent: float
    gate_passed: bool
    total_seconds_runs: tuple[float, float, float]
    total_seconds_delta_percent: float
    total_seconds_gate_threshold_percent: float
    total_seconds_gate_passed: bool
    total_seconds_p50: float
    total_seconds_p95: float
    total_seconds_gate_mode: str
    total_seconds_delta_abs_seconds: float


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = PROJECT_ROOT / "tests" / "fixtures" / "benchmark_fixture_contract.json"
EVIDENCE_PATH = (
    PROJECT_ROOT / ".sisyphus" / "evidence" / "stabilization-memory-benchmark.txt"
)
RUN_COUNT = 3
DELTA_GATE_PERCENT = 15.0
TOTAL_SECONDS_DELTA_GATE_PERCENT = 20.0
TOTAL_SECONDS_GATE_MIN_BASELINE_SECONDS = 1.0
TOTAL_SECONDS_DELTA_GATE_ABS_SECONDS = 0.150
RSS_PATTERN = re.compile(
    r"^\s*Maximum resident set size \(kbytes\):\s*(?P<rss>\d+)\s*$",
    re.MULTILINE,
)
STAGE_TIMING_PATTERN = re.compile(
    r"Diagnostics:\s*mode=stage_timing\s+"
    r"native_seconds=(?P<native>[0-9]+(?:\.[0-9]+)?)\s+"
    r"ocr_seconds=(?P<ocr>[0-9]+(?:\.[0-9]+)?)\s+"
    r"postprocess_seconds=(?P<postprocess>[0-9]+(?:\.[0-9]+)?)\s+"
    r"write_seconds=(?P<write>[0-9]+(?:\.[0-9]+)?)\s+"
    r"total_seconds=(?P<total>[0-9]+(?:\.[0-9]+)?)"
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fixture_file:
        for chunk in iter(lambda: fixture_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_fixture_contract() -> FixtureContract:
    contract = cast(
        ContractFile,
        json.loads(CONTRACT_PATH.read_text(encoding="utf-8")),
    )
    return contract["canonical_benchmark_fixture"]


def _benchmark_command(fixture_path: str) -> list[str]:
    return [
        "/usr/bin/time",
        "-v",
        ".venv/bin/python",
        "pdf_to_md.py",
        fixture_path,
        "-o",
        "/tmp/large.md",
        "--force",
        "--ocr",
        "auto",
        "--ocr-engine",
        "rapidocr",
    ]


def _extract_rss_kb(time_stderr: str) -> int:
    match = RSS_PATTERN.search(time_stderr)
    if match is None:
        raise RuntimeError(
            "failed to parse '/usr/bin/time -v' output: missing 'Maximum resident set size (kbytes)'"
        )
    return int(match.group("rss"))


def _extract_stage_timing(stderr_text: str) -> tuple[float, float, float, float, float]:
    matched = list(STAGE_TIMING_PATTERN.finditer(stderr_text))
    if not matched:
        raise RuntimeError("failed to parse stage timing diagnostics from stderr")
    payload = matched[-1]
    return (
        float(payload.group("native")),
        float(payload.group("ocr")),
        float(payload.group("postprocess")),
        float(payload.group("write")),
        float(payload.group("total")),
    )


def _run_benchmark_three_times(fixture_path: str) -> tuple[BenchmarkRun, BenchmarkRun, BenchmarkRun]:
    runs: list[BenchmarkRun] = []
    command = _benchmark_command(fixture_path)
    for index in range(1, RUN_COUNT + 1):
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"canonical benchmark command failed on run {index} (exit {completed.returncode})"
            )
        (
            stage_native_seconds,
            stage_ocr_seconds,
            stage_postprocess_seconds,
            stage_write_seconds,
            stage_total_seconds,
        ) = _extract_stage_timing(completed.stderr)
        runs.append(
            BenchmarkRun(
                index=index,
                rss_kb=_extract_rss_kb(completed.stderr),
                stage_native_seconds=stage_native_seconds,
                stage_ocr_seconds=stage_ocr_seconds,
                stage_postprocess_seconds=stage_postprocess_seconds,
                stage_write_seconds=stage_write_seconds,
                stage_total_seconds=stage_total_seconds,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        )

    return cast(tuple[BenchmarkRun, BenchmarkRun, BenchmarkRun], tuple(runs))


def _delta_percent(rss_kb_runs: tuple[int, int, int]) -> float:
    max_rss = max(rss_kb_runs)
    min_rss = min(rss_kb_runs)
    if max_rss == 0:
        return 0.0
    return ((max_rss - min_rss) / max_rss) * 100.0


def _delta_percent_float(values: tuple[float, float, float]) -> float:
    max_value = max(values)
    min_value = min(values)
    if max_value <= 0.0:
        return 0.0
    return ((max_value - min_value) / max_value) * 100.0


def _delta_abs_seconds(values: tuple[float, float, float]) -> float:
    return max(values) - min(values)


def _compute_p50(values: tuple[float, float, float]) -> float:
    ordered = sorted(values)
    return ordered[1]


def _compute_p95(values: tuple[float, float, float]) -> float:
    ordered = sorted(values)
    return ordered[-1]


def run_reproducibility_check() -> ReproducibilityResult:
    fixture_contract = _load_fixture_contract()
    fixture_path = fixture_contract["path"]
    fixture_file = PROJECT_ROOT / fixture_path

    actual_sha256 = _sha256_file(fixture_file)
    actual_bytes = fixture_file.stat().st_size
    if actual_sha256 != fixture_contract["sha256"] or actual_bytes != fixture_contract["bytes"]:
        raise RuntimeError("canonical fixture identity mismatch with benchmark contract")

    runs = _run_benchmark_three_times(fixture_path)
    rss_kb_runs = cast(tuple[int, int, int], tuple(run.rss_kb for run in runs))
    delta = _delta_percent(rss_kb_runs)
    gate_passed = delta <= DELTA_GATE_PERCENT
    total_seconds_runs = cast(
        tuple[float, float, float],
        tuple(run.stage_total_seconds for run in runs),
    )
    total_seconds_delta_percent = _delta_percent_float(total_seconds_runs)
    total_seconds_delta_abs_seconds = _delta_abs_seconds(total_seconds_runs)
    total_seconds_gate_mode = "percent"
    if max(total_seconds_runs) < TOTAL_SECONDS_GATE_MIN_BASELINE_SECONDS:
        total_seconds_gate_mode = "absolute"
        total_seconds_gate_passed = (
            total_seconds_delta_abs_seconds <= TOTAL_SECONDS_DELTA_GATE_ABS_SECONDS
        )
    else:
        total_seconds_gate_passed = total_seconds_delta_percent <= TOTAL_SECONDS_DELTA_GATE_PERCENT
    total_seconds_p50 = _compute_p50(total_seconds_runs)
    total_seconds_p95 = _compute_p95(total_seconds_runs)

    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    evidence_lines = [
        "Task 4 Reproducibility Evidence",
        f"benchmark_command: {' '.join(_benchmark_command(fixture_path))}",
        f"fixture_path: {fixture_path}",
        f"fixture_sha256: {fixture_contract['sha256']}",
        f"fixture_bytes: {fixture_contract['bytes']}",
        f"run_1_rss_kb: {rss_kb_runs[0]}",
        f"run_2_rss_kb: {rss_kb_runs[1]}",
        f"run_3_rss_kb: {rss_kb_runs[2]}",
        f"rss_max_kb: {max(rss_kb_runs)}",
        f"rss_min_kb: {min(rss_kb_runs)}",
        "delta_formula_percent: ((max_rss_kb-min_rss_kb)/max_rss_kb)*100",
        f"rss_delta_percent: {delta:.6f}",
        f"gate_threshold_percent: {DELTA_GATE_PERCENT:.2f}",
        f"reproducibility_gate: {'PASS' if gate_passed else 'FAIL'}",
        f"run_1_total_seconds: {total_seconds_runs[0]:.3f}",
        f"run_2_total_seconds: {total_seconds_runs[1]:.3f}",
        f"run_3_total_seconds: {total_seconds_runs[2]:.3f}",
        "total_seconds_delta_formula_percent: ((max_total_seconds-min_total_seconds)/max_total_seconds)*100",
        f"total_seconds_delta_percent: {total_seconds_delta_percent:.6f}",
        f"total_seconds_delta_abs_seconds: {total_seconds_delta_abs_seconds:.6f}",
        f"total_seconds_gate_mode: {total_seconds_gate_mode}",
        f"total_seconds_gate_min_baseline_seconds: {TOTAL_SECONDS_GATE_MIN_BASELINE_SECONDS:.3f}",
        f"total_seconds_gate_abs_threshold_seconds: {TOTAL_SECONDS_DELTA_GATE_ABS_SECONDS:.3f}",
        f"total_seconds_gate_threshold_percent: {TOTAL_SECONDS_DELTA_GATE_PERCENT:.2f}",
        f"total_seconds_reproducibility_gate: {'PASS' if total_seconds_gate_passed else 'FAIL'}",
        f"total_seconds_p50: {total_seconds_p50:.3f}",
        f"total_seconds_p95: {total_seconds_p95:.3f}",
        "",
    ]

    for run in runs:
        evidence_lines.append(f"--- run_{run.index}_stdout ---")
        evidence_lines.append(run.stdout.rstrip("\n"))
        evidence_lines.append("")
        evidence_lines.append(f"--- run_{run.index}_time_stderr ---")
        evidence_lines.append(run.stderr.rstrip("\n"))
        evidence_lines.append("")

    _ = EVIDENCE_PATH.write_text("\n".join(evidence_lines), encoding="utf-8")

    return ReproducibilityResult(
        artifact_path=EVIDENCE_PATH,
        fixture_path=fixture_path,
        fixture_sha256=fixture_contract["sha256"],
        fixture_bytes=fixture_contract["bytes"],
        rss_kb_runs=rss_kb_runs,
        rss_delta_percent=delta,
        gate_threshold_percent=DELTA_GATE_PERCENT,
        gate_passed=gate_passed,
        total_seconds_runs=total_seconds_runs,
        total_seconds_delta_percent=total_seconds_delta_percent,
        total_seconds_gate_threshold_percent=TOTAL_SECONDS_DELTA_GATE_PERCENT,
        total_seconds_gate_passed=total_seconds_gate_passed,
        total_seconds_p50=total_seconds_p50,
        total_seconds_p95=total_seconds_p95,
        total_seconds_gate_mode=total_seconds_gate_mode,
        total_seconds_delta_abs_seconds=total_seconds_delta_abs_seconds,
    )
