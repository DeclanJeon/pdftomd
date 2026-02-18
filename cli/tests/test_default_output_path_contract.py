from __future__ import annotations

from pathlib import Path

import pdf_cli
import pdf_to_md


def test_pdf_to_md_default_output_path_uses_project_downloads(
    monkeypatch,
    tmp_path: Path,
) -> None:
    input_pdf = tmp_path / "sample.pdf"
    monkeypatch.setattr(pdf_to_md, "_resolve_project_root", lambda: tmp_path)

    assert pdf_to_md._resolve_default_output_path(input_pdf) == (
        tmp_path / "downloads" / "sample" / "sample.md"
    )


def test_pdf_cli_output_arg_defaults_to_project_downloads(
    monkeypatch,
    tmp_path: Path,
) -> None:
    input_pdf = Path("sample.pdf")
    monkeypatch.setattr(pdf_cli, "_resolve_project_root", lambda: tmp_path)

    assert pdf_cli._resolve_output_arg([str(input_pdf)], input_pdf) == str(
        tmp_path / "downloads" / "sample" / "sample.md"
    )


def test_pdf_cli_output_arg_keeps_explicit_output(monkeypatch, tmp_path: Path) -> None:
    input_pdf = Path("sample.pdf")
    monkeypatch.setattr(pdf_cli, "_resolve_project_root", lambda: tmp_path)

    assert pdf_cli._resolve_output_arg([str(input_pdf), "-o", "custom.md"], input_pdf) == "custom.md"
