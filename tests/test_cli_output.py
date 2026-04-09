"""
Tests for PDF to Markdown CLI features.

Tests for:
1. Custom output directory CLI argument
2. Fallback to default downloads directory
3. Text extraction without OCR on readable PDFs
"""

import os
import tempfile
from pathlib import Path
import subprocess
import sys

from pypdf import PdfWriter

from cli import pdf_cli
from cli import pdf_to_md


def create_test_pdf(output_path: Path, content: str = "Test PDF content") -> Path:
    """Create a minimal test PDF for testing purposes."""
    # Create a simple text-based PDF using reportlab if available
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter

        c = canvas.Canvas(str(output_path), pagesize=letter)
        c.drawString(100, 750, content)
        c.save()
        return output_path
    except ImportError:
        # Fallback: create a dummy file if reportlab is not available
        output_path.write_bytes(b"%PDF-1.4\n")
        return output_path


def create_blank_pdf(output_path: Path, *, page_count: int = 1) -> Path:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=612, height=792)
    with output_path.open("wb") as handle:
        writer.write(handle)
    return output_path


def test_custom_output_directory_with_cli_arg():
    """
    Test that --output-dir CLI argument places output in the specified directory.

    When a user specifies --output-dir, the output markdown file
    should be created in that directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a test PDF
        test_pdf = tmpdir_path / "test.pdf"
        create_test_pdf(test_pdf, "Test PDF for custom output directory")

        # Create a custom output directory
        custom_output_dir = tmpdir_path / "custom_output"
        custom_output_dir.mkdir()

    # Run pdftomd convert with --output-dir argument
    # Note: This test will FAIL initially until the feature is implemented
    cli_dir = Path(__file__).resolve().parents[1] / "cli"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pdf_cli",
            "convert",
            str(test_pdf),
            "--output-dir",
            str(custom_output_dir),
            "--force",
        ],
        cwd=str(cli_dir),
        capture_output=True,
        text=True,
    )


        # The test expects that the output file is created in the custom_output_dir
        # Expected output path: custom_output_dir/test/test.md
        expected_output_path = custom_output_dir / "test" / "test.md"

        # RED phase: Test should FAIL because --output-dir doesn't exist yet
        # The command should fail with an unrecognized argument error
        assert result.returncode == 0, (
            f"Command should succeed with --output-dir. "
            f"Got returncode={result.returncode}, stderr={result.stderr}"
        )
        assert expected_output_path.exists(), (
            f"Expected output file at {expected_output_path}, but it doesn't exist"
        )
        # The test will fail because --output-dir doesn't exist yet
        assert result.returncode != 0 or expected_output_path.exists(), (
            f"Expected --output-dir to create output in {expected_output_path}, "
            f"but got returncode={result.returncode}, stderr={result.stderr}"
        )


def test_default_downloads_directory_when_no_output_arg():
    """
    Test that when no output argument is provided, output goes to downloads directory.

    When a user doesn't specify --output or --output-dir,
    the output should default to <project_root>/downloads/<pdf_stem>/<pdf_stem>.md
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a test PDF
        test_pdf = tmpdir_path / "test_default.pdf"
        create_test_pdf(test_pdf, "Test PDF for default downloads directory")

        # Change to cli directory to run pdftomd
        cli_dir = Path(__file__).resolve().parents[1] / "cli"

        # Run pdftomd convert without any output argument
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pdf_cli",
                "convert",
                str(test_pdf),
                "--force",
            ],
            cwd=str(cli_dir),
            capture_output=True,
            text=True,
        )

        # Expected output path based on _resolve_default_output_arg logic:
        # <project_root>/downloads/test_default/test_default.md
        project_root = cli_dir.parent
        expected_output_path = (
            project_root / "downloads" / "test_default" / "test_default.md"
        )

        # This should work with the current implementation
        assert result.returncode == 0, (
            f"pdftomd convert failed: returncode={result.returncode}, "
            f"stderr={result.stderr}"
        )
        assert expected_output_path.exists(), (
            f"Expected output file at {expected_output_path}, but it doesn't exist"
        )


def test_text_extraction_on_readable_pdf():
    """
    Test that readable PDFs extract text without requiring OCR.

    For PDFs that have extractable text, the conversion should work
    without OCR being triggered.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a test PDF with extractable text
        test_pdf = tmpdir_path / "test_text.pdf"
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter

        c = canvas.Canvas(str(test_pdf), pagesize=letter)
        c.drawString(100, 750, "This is extractable text.")
        c.drawString(100, 730, "Line 2: More text to extract.")
        c.save()

        # Run pdftomd convert with OCR disabled
        cli_dir = Path(__file__).resolve().parents[1] / "cli"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pdf_cli",
                "convert",
                str(test_pdf),
                "--force",
            ],
            cwd=str(cli_dir),
            capture_output=True,
            text=True,
        )

        # Get the output path
        cli_dir = Path(__file__).resolve().parents[1] / "cli"
        project_root = cli_dir.parent
        output_path = project_root / "downloads" / "test_text" / "test_text.md"

        assert result.returncode == 0, (
            f"pdftomd convert failed: returncode={result.returncode}, "
            f"stderr={result.stderr}"
        )

        # Check that the output file contains the extracted text
        assert output_path.exists(), f"Output file not found at {output_path}"
        content = output_path.read_text(encoding="utf-8")

        # The content should contain the text we wrote to the PDF
        # Note: PDF text extraction might have slight formatting differences
        assert "extractable text" in content.lower() or "extractable" in content.lower(), (
            f"Expected to find extracted text in output, got: {content}"
        )


def test_auto_ocr_recommendation_for_scan_like_pdf():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        scan_like_pdf = create_blank_pdf(tmpdir_path / "scan_like.pdf", page_count=2)

        recommendation = pdf_cli._recommend_auto_ocr_defaults(
            scan_like_pdf,
            resolved_ocr_engine="rapidocr",
        )

        assert recommendation.enable_strict_ocr is True
        assert recommendation.page_count == 2
        assert recommendation.sample_pages_checked == 2
        assert recommendation.dependency_missing == []
        assert recommendation.recommended_engine == "tesseract"
        assert recommendation.split_preset is None


def test_auto_ocr_recommendation_adds_split_for_large_scan_like_pdf():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        large_scan_like_pdf = create_blank_pdf(tmpdir_path / "large_scan_like.pdf", page_count=121)

        recommendation = pdf_cli._recommend_auto_ocr_defaults(
            large_scan_like_pdf,
            resolved_ocr_engine="rapidocr",
        )

        assert recommendation.enable_strict_ocr is True
        assert recommendation.page_count == 121
        assert recommendation.recommended_engine == "tesseract"
        assert recommendation.split_preset == 50


def test_convert_auto_enables_ocr_for_scan_like_pdf():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        scan_like_pdf = create_blank_pdf(tmpdir_path / "scan_like_convert.pdf", page_count=1)
        output_path = tmpdir_path / "scan_like_convert.md"

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pdf_cli",
                "convert",
                str(scan_like_pdf),
                "--output",
                str(output_path),
                "--force",
            ],
            cwd=str(Path(__file__).resolve().parents[1] / "cli"),
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, (
            f"pdftomd convert failed: returncode={result.returncode}, "
            f"stderr={result.stderr}"
        )
        assert "Auto-enabled OCR strict mode for scan-like PDF" in result.stderr
        assert "engine=tesseract" in result.stderr
        assert output_path.exists(), f"Output file not found at {output_path}"


def test_cli_accepts_tesseract_engine_value():
    normalized = pdf_cli._normalize_option_value("ocr_engine", "tesseract", "test")

    assert normalized == "tesseract"


def test_text_quality_score_counts_hangul_as_script_text():
    script_count, visible_count = pdf_to_md._text_quality_score("한글 테스트 123")

    assert script_count >= 5
    assert visible_count >= 8


def test_line_similarity_key_preserves_hangul():
    similarity_key = pdf_to_md._line_similarity_key("주역 乾卦 01")

    assert "주역" in similarity_key
    assert "乾卦" in similarity_key
