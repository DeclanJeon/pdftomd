# PDF to Markdown Converter

Convert PDF pages to Markdown with a memory-bounded, page-streaming pipeline.

## Features

- Streaming markdown writer with stable `# Page N` headers.
- Staged routing for native text, layout-assisted fallback, and OCR fallback.
- RapidOCR-only OCR backend with deterministic diagnostics.
- Hybrid CLI surface: `pdftomd convert|init|config|profile`.
- Legacy conversion path remains available in v1: `python pdf_to_md.py ...`.
- Interactive `--wizard` and `--ctl` modes with non-TTY-safe behavior.

## Requirements

- Python 3.10+

## Installation

### Quick Install (One Command)

Install the CLI with OCR support:

```bash
cd cli
python -m pip install -U pip
python -m pip install .[full]
```

Or install directly from repo root:

```bash
python -m pip install ./cli[full]
```

If you want only the CLI core (no OCR):

```bash
cd cli
python -m pip install .
```

### Linux (Ubuntu/Debian)

```bash
# Install Python 3.10+ (if needed)
sudo apt update
sudo apt install -y python3.10 python3.10-venv python3.10-dev

# Install Poppler (required for OCR)
sudo apt install -y poppler-utils

# Create virtual environment and install
cd cli
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt

# Verify installation
pdftomd --help
```

### macOS

```bash
# Install Python via Homebrew (if needed)
brew install python@3.11

# Install Poppler (required for OCR)
brew install poppler

# Create virtual environment and install
cd cli
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt

# Verify installation
pdftomd --help
```

### Windows

```powershell
# Install Python 3.10+ from https://www.python.org/downloads/
# Check "Add Python to PATH" during installation

# Install Poppler for OCR (using Conda - recommended)
conda install -c conda-forge poppler

# Or download Poppler for Windows manually:
# https://github.com/oschwartz10612/poppler-windows/releases/
# Extract and add to PATH: C:\Program Files\poppler\Library\bin

# Create virtual environment
cd cli
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install packages
python -m pip install -U pip
python -m pip install -r requirements.txt

# Verify installation
pdftomd --help
```

If you get PowerShell execution policy error:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## Usage

### Hybrid CLI (v1 default)

```bash
pdftomd --help
pdftomd convert --help
pdftomd init
pdftomd config show
pdftomd profile list
```

### Basic Conversion

```bash
# Simple conversion (native text only)
pdftomd convert test.pdf -o test.md --force

# With OCR fallback
pdftomd convert test.pdf --ocr-fallback --ocr-engine rapidocr -o test.md --force

# With multiple workers
pdftomd convert test.pdf --workers 4 -o test-workers.md --force

# Split output into chunks (10 pages each)
pdftomd convert test.pdf --split-preset 10 -o test.md --force

# Split output every 20 pages with parallel OCR
pdftomd convert test.pdf --split-every 20 --ocr auto --split-ocr-parallel --ocr-engine rapidocr -o test.md --force
```

### Interactive Modes

Start interactive wizard:

```bash
pdftomd convert --wizard
```

Wizard presets:
- `fast`: native-first, OCR fallback off
- `balanced`: OCR fallback on, `rapidocr` backend
- `accurate`: OCR fallback on, `rapidocr` backend

Start full terminal interactive mode (CTL style):

```bash
pdftomd convert --ctl
```

### Legacy Path (still supported in v1)

```bash
.venv/bin/python pdf_to_md.py test.pdf -o test.md --force
```

Legacy-style hybrid invocation with deprecation warning:

```bash
pdftomd test.pdf -o test.md --force
```

## Engine Routing

The converter uses deterministic routing in this order:

| Stage | Trigger | Engine | Scope |
| --- | --- | --- | --- |
| Native text | Always | `pdfminer.six` | All pages |
| Layout assist | `--ocr-fallback` and weak page text | `pdfplumber` | Weak pages only (bounded) |
| OCR fallback | `--ocr-fallback` and weak pages remain | `rapidocr` | Weak pages only (windowed) |

Note: `--ocr-engine` accepts only `rapidocr`.

## Config and Profiles

The hybrid CLI resolves conversion options with this precedence:

```
CLI > env > profile/config > defaults
```

```bash
# Initialize config
pdftomd init

# Config management
pdftomd config validate
pdftomd config show

# Profile management
pdftomd profile list
pdftomd profile set team ocr_mode strict
pdftomd profile use team

# Use profile
pdftomd convert input.pdf --profile team -o out.md --force
```

Environment variables:

- `PDF_TO_MD_CONFIG`, `PDF_TO_MD_OUTPUT`, `PDF_TO_MD_FORCE`
- `PDF_TO_MD_OCR_MODE`, `PDF_TO_MD_OCR_ENGINE`, `PDF_TO_MD_OCR_LAYOUT`
- `PDF_TO_MD_CLASSICAL_ZH_POSTPROCESS`, `PDF_TO_MD_KEY_CONTENT_FALLBACK`, `PDF_TO_MD_PROFILE`
- `PDF_TO_MD_SPLIT_PRESET` (10/20/50/100), `PDF_TO_MD_SPLIT_EVERY` (positive int)
- `PDF_TO_MD_WORKERS` (positive int)

## Error Behavior

- No-arg usage error -> exit code `2`
- Missing input PDF file -> exit code `1`
- Output exists without `--force` -> exit code `1`
- Conversion failure -> exit code `1` with `Conversion failed:` prefix
- Runtime config/profile validation failure -> exit code `1`

## Troubleshooting

### `pdftoppm not found` error

Poppler is not installed or not in PATH:

- Linux: `sudo apt install poppler-utils`
- macOS: `brew install poppler`
- Windows: See Windows installation section above

### Memory issues with large PDFs

Reduce memory usage with these options:

```bash
# Disable parallel processing
pdftomd convert large.pdf --workers 1 -o output.md --force

# Split into smaller chunks
pdftomd convert large.pdf --split-every 10 -o output.md --force
```

### OCR quality issues

For better OCR quality:

```bash
# For vertical text documents
pdftomd convert input.pdf --ocr-fallback --ocr-engine rapidocr --ocr-layout vertical -o output.md --force

# For classical Chinese/ancient documents
pdftomd convert input.pdf --ocr-fallback --ocr-engine rapidocr --ocr-classical-zh-postprocess -o output.md --force

# Combined options
pdftomd convert input.pdf --ocr-fallback --ocr-engine rapidocr --ocr-layout vertical --ocr-classical-zh-postprocess --ocr-key-content-fallback -o output.md --force
```
