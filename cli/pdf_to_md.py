from __future__ import annotations

import argparse
import errno
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import lru_cache
import importlib
import importlib.util
import io
from collections.abc import Iterable, Iterator
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Callable, Protocol, TypedDict, cast


class _ExtractPages(Protocol):
    def __call__(
        self,
        pdf_file: str,
        *,
        maxpages: int = 0,
    ) -> Iterator["_PageLayout"]: ...


class _PageLayout(Protocol):
    def __iter__(self) -> Iterator[object]: ...


class _ConvertFromPath(Protocol):
    def __call__(
        self,
        pdf_path: str,
        *,
        first_page: int,
        last_page: int,
        dpi: int,
        size: tuple[int, int],
        thread_count: int = 1,
        grayscale: bool = False,
        use_pdftocairo: bool = False,
        timeout: int | None = None,
    ) -> list[object]: ...


class _ImageToString(Protocol):
    def __call__(self, image: object) -> str: ...


class _HasGetText(Protocol):
    def get_text(self) -> str: ...


class _PdfPlumberPage(Protocol):
    def extract_text(self, *, layout: bool = False) -> str | None: ...


class _PdfPlumberDocument(Protocol):
    pages: list[_PdfPlumberPage]

    def __enter__(self) -> "_PdfPlumberDocument": ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool | None: ...


class _PdfPlumberOpen(Protocol):
    def __call__(self, pdf_path: str) -> _PdfPlumberDocument: ...


class _OcrLineMetadata(TypedDict):
    text: str
    conf: float
    bbox: tuple[float, float, float, float]
    engine: str
    variant: str


class _ProgressEvent(TypedDict):
    percent: int
    stage: str


_PageProgressCallback = Callable[[int, int], None]

PROGRESS_FORMAT_TEXT = "text"
PROGRESS_FORMAT_JSONL = "jsonl"
PROGRESS_FORMAT_CHOICES: tuple[str, ...] = (
    PROGRESS_FORMAT_TEXT,
    PROGRESS_FORMAT_JSONL,
)
_active_progress_format = PROGRESS_FORMAT_TEXT


OPTIONAL_BACKENDS: tuple[str, ...] = ()
WIZARD_IMPLEMENTED_OCR_ENGINES: tuple[str, ...] = ("rapidocr",)

PDFPLUMBER_TRIGGER_MIN_CHAR_COUNT = 20
PDFPLUMBER_TRIGGER_MIN_PRINTABLE_RATIO = 0.85
PDFPLUMBER_MAX_PAGES_PER_DOCUMENT = 8

OCR_DEFAULT_ENGINE = "rapidocr"
OCR_PAGE_WINDOW_SIZE = 2
OCR_MAX_DPI = 200
OCR_MAX_IMAGE_SIZE = (1800, 1800)
OCR_MIN_CONFIDENCE = 0.45
OCR_HIGH_CONF_MIN_RATIO = 0.60
OCR_PAGE_SCORE_CONFIDENCE_VISIBLE_TARGET = 40
OCR_PAGE_SCORE_REPLACEMENT_MARGIN = 0.08
OCR_PAGE_SCORE_CONFIDENCE_WEIGHT = 0.75
OCR_PAGE_SCORE_CJK_RATIO_WEIGHT = 0.25
OCR_PAGE_SCORE_NOISE_PENALTY_WEIGHT = 0.50
OCR_PAGE_SCORE_DUPLICATE_PENALTY_WEIGHT = 0.40
OCR_PAGE_SCORE_NOISE_THRESHOLD = 0.20
OCR_PAGE_SCORE_FALLBACK_FLOOR = 0.10
OCR_CLASSICAL_ZH_AGGRESSIVE_SCORE_THRESHOLD = 0.32
OCR_KEY_CONTENT_MAX_LINES = 5
OCR_KEY_CONTENT_MIN_PAGE_SCORE = 0.25
OCR_TESSERACT_MIN_WORD_CONFIDENCE = 40.0
OCR_TESSERACT_MIN_LINE_CONFIDENCE = 48.0
OCR_WEAK_PAGE_PRESET_DPIS: tuple[int, ...] = (OCR_MAX_DPI, 240)
OCR_SHORT_CJK_FRAGMENT_WHITELIST: frozenset[str] = frozenset(
    {
        "之",
        "其",
        "也",
        "矣",
        "乎",
        "焉",
        "者",
        "曰",
        "云",
        "耳",
        "夫",
        "盖",
    }
)
OCR_SHORT_CJK_FRAGMENT_STRIP_CHARS = "，。！？；：、「」『』（）()《》〈〉【】〔〕"
OCR_MODE_AUTO = "auto"
OCR_LAYOUT_AUTO = "auto"
OCR_LAYOUT_VERTICAL = "vertical"
OCR_LAYOUT_HORIZONTAL = "horizontal"
OCR_PDF2IMAGE_TIMEOUT_SECONDS = 120
OCR_PDF2IMAGE_THREAD_COUNT_SINGLE_WORKER_CAP = 4
OCR_PDF2IMAGE_USE_GRAYSCALE = True
OCR_PDF2IMAGE_USE_PDFTOCAIRO = True
OCR_SIMILARITY_MAX_BUCKET_CANDIDATES = 24
OCR_TRANSIENT_RETRY_MAX_ATTEMPTS = 3
OCR_TRANSIENT_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (0.2, 0.5)
ZH_SCRIPT_KEEP = "keep"
ZH_SCRIPT_HANT = "hant"
ZH_SCRIPT_HANS = "hans"
ZH_SCRIPT_CHOICES: tuple[str, ...] = (
    ZH_SCRIPT_KEEP,
    ZH_SCRIPT_HANT,
    ZH_SCRIPT_HANS,
)
_OPENCC_CONFIG_BY_ZH_SCRIPT: dict[str, str] = {
    ZH_SCRIPT_HANT: "s2t",
    ZH_SCRIPT_HANS: "t2s",
}
SPLIT_PRESET_CHOICES: tuple[int, ...] = (10, 20, 50, 100)
_RESOURCE_USAGE_LIMIT = 0.85
_RESOURCE_POLL_SECONDS = 0.2
_RESOURCE_WAIT_HEARTBEAT_SECONDS = 5.0
_RESOURCE_WAIT_MAX_SECONDS = 60.0
_RESOURCE_WAIT_FAIL_OPEN = True
_RESOURCE_GUARD_AUTO_FAIL_CLOSED_PAGE_THRESHOLD = 120
_RESOURCE_GUARD_POLICY_FAIL_OPEN = "fail-open"
_RESOURCE_GUARD_POLICY_FAIL_CLOSED = "fail-closed"
_RESOURCE_GUARD_POLICY_CHOICES: tuple[str, ...] = (
    _RESOURCE_GUARD_POLICY_FAIL_OPEN,
    _RESOURCE_GUARD_POLICY_FAIL_CLOSED,
)
_DEFAULT_DOWNLOADS_DIR_NAME = "downloads"
_ocr_extractor_cache_enabled = False
_OCR_EXTRACTOR_CACHE: dict[tuple[str, str, str], _ImageToString] = {}
_PERF_REPORT_PATH_DEFAULT = Path("report/perf_last_run.md")

_OCR_THRESHOLD_PROFILE_OVERRIDES: dict[str, dict[str, float]] = {
    "default": {},
    "rapidocr": {
        "replacement_margin": 0.06,
        "fallback_floor": 0.08,
    },
}

_OCR_LAYOUT_THRESHOLD_OVERRIDES: dict[str, dict[str, float]] = {
    OCR_LAYOUT_AUTO: {},
    OCR_LAYOUT_VERTICAL: {
        "replacement_margin": 0.05,
        "noise_threshold": 0.18,
    },
    OCR_LAYOUT_HORIZONTAL: {
        "replacement_margin": 0.09,
    },
}

CLASSICAL_ZH_PHRASE_CORRECTIONS: tuple[tuple[str, str], ...] = (
    ("林玄解叙", "豪林玄解敘"),
    ("本能傅兹者何先生山家藏晋仙翁秘本加泰。", "先生山家藏晉仙翁秘本"),
    ("项镶而见马覺而憶之其器", "覺而憶之其間"),
    ("一人八物物事", "一人一物一事"),
    ("与境往往變茫无足", "與境往往變亂紛紜無足"),
    ("與境往往變鼠洁无足", "與境往往變亂紛紜無足"),
    ("舟飞於睦鼠化牛龍", "舟飛於嶽陸鼠化爲牛龍"),
    ("舟飛於岭隆鼠化马牛龍", "舟飛於嶽陸鼠化爲牛龍"),
    ("合也夢非真平爱人见是", "謂夢非真乎夢人見是"),
    ("合也夢非真乎夢人见是", "謂夢非真乎夢人見是"),
    ("成汤见負鼎叔孫牛於", "成湯見負鼎叔孫識豎牛於"),
    ("夢曰夢是六夢六夢之", "是謂六夢六夢之變"),
    ("华蛋", "華胥"),
    ("咸账", "咸陟"),
    ("刘職", "列職"),
    ("远益", "近蓋"),
    ("里王", "聖王"),
    ("巳作圖", "已作圖"),
    ("日正夢", "曰正夢"),
    ("日藍夢", "曰噩夢"),
    ("日思夢", "曰思夢"),
    ("日夢日喜", "曰寤夢曰喜"),
    ("木能傳兹者何問卿先生曲家藏晋葛秘本最加泰", "本能傳兹者何問卿先生山家藏晉仙翁秘本加泰"),
    ("占一书圆經既于泰炬业復千欢罕嘴类", "夢占一書圓經既於秦漢術業復於漠儒致真罕覯類"),
    ("茂金害仰览者有一旁总有一古在一", "成全書仰覽者有一夢必有一占有一占必有一驗"),
    ("一鑫诚", "一驗誠"),
    ("豪林玄解叙", "夢林玄解敘"),
    ("调夢真乎人人物物事事", "也謂夢真乎一人一物一事"),
    ("舟飞於鼠化牛龍", "舟飛於嶽陸鼠化爲牛龍"),
    ("成湯见負鼎叔孫識牛", "成湯見負鼎叔孫識豎牛"),
    ("近益自隆古里王亦已作圖", "近蓋自隆古聖王亦已作圖"),
    ("蔓日夢是六夢六夢之", "曰噩夢是六夢六夢之"),
    ("蔓占", "夢占"),
    ("萝占", "夢占"),
    ("傅夢", "傳夢"),
    ("圆圖", "圓圖"),
    ("圆説", "圓說"),
    ("休答", "休咎"),
    ("君臣父母\n春戚麟里", "君臣父母\n親戚鄰里"),
)

CLASSICAL_ZH_AGGRESSIVE_CORRECTIONS: tuple[tuple[str, str], ...] = (
    ("Ihklt k.", ""),
    ("afek lae N--", ""),
    ("1111", ""),
)

_OPTIONAL_BACKEND_MODULES: dict[str, tuple[str, ...]] = {}

_OPTIONAL_BACKEND_GUIDANCE: dict[str, str] = {}

_DEPENDENCY_INSTALL_HINT_BY_ENGINE: dict[str, str] = {
    "rapidocr": "`pdf2image`, `rapidocr_onnxruntime`, and Poppler tools",
}


@dataclass(frozen=True)
class WizardPreset:
    ocr_fallback: bool
    ocr_engine: str


@dataclass(frozen=True)
class OcrFallbackPipelineResult:
    page_texts: list[str]
    weak_pages_before_pdfplumber: int
    weak_pages_after_pdfplumber: int
    ocr_pages_requested: int
    ocr_pages_applied: int
    resolved_ocr_engine: str
    should_abort: bool


WIZARD_PRESET_PROFILES: dict[str, WizardPreset] = {
    "fast": WizardPreset(ocr_fallback=False, ocr_engine="rapidocr"),
    "balanced": WizardPreset(ocr_fallback=True, ocr_engine="rapidocr"),
    "accurate": WizardPreset(ocr_fallback=True, ocr_engine="rapidocr"),
}


def _normalize_page_text(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    normalized = "\n".join(lines).strip()
    return normalized


def _text_quality_score(text: str) -> tuple[int, int]:
    cjk_count = sum(1 for char in text if "\u4e00" <= char <= "\u9fff")
    visible_count = sum(1 for char in text if not char.isspace())
    return cjk_count, visible_count


def _line_similarity_key(text: str) -> str:
    return "".join(
        character
        for character in text
        if ("\u4e00" <= character <= "\u9fff") or character.isalnum()
    )


def _line_similarity_bucket(text: str) -> int:
    similarity_key = _line_similarity_key(text)
    return len(similarity_key) // 4


def _iter_similarity_bucket_candidates(
    bucket_to_lines: dict[int, list[str]],
    text: str,
) -> Iterator[str]:
    base_bucket = _line_similarity_bucket(text)
    gathered: list[str] = []
    for bucket in (base_bucket - 1, base_bucket, base_bucket + 1):
        gathered.extend(bucket_to_lines.get(bucket, []))

    if len(gathered) <= OCR_SIMILARITY_MAX_BUCKET_CANDIDATES:
        for candidate in gathered:
            yield candidate
        return

    target_length = len(text)
    ranked = sorted(gathered, key=lambda candidate: abs(len(candidate) - target_length))
    for candidate in ranked[:OCR_SIMILARITY_MAX_BUCKET_CANDIDATES]:
        yield candidate


def _is_noise_line(text: str) -> bool:
    stripped_text = text.strip()
    if not stripped_text:
        return True

    def _is_whitelisted_short_cjk_fragment(value: str) -> bool:
        normalized = "".join(
            character
            for character in value
            if (not character.isspace())
            and (character not in OCR_SHORT_CJK_FRAGMENT_STRIP_CHARS)
        )
        if len(normalized) != 1:
            return False
        return normalized in OCR_SHORT_CJK_FRAGMENT_WHITELIST

    cjk_count, visible_count = _text_quality_score(stripped_text)
    latin_count = sum(1 for character in stripped_text if "a" <= character.lower() <= "z")
    digit_count = sum(1 for character in stripped_text if character.isdigit())
    symbol_count = sum(
        1
        for character in stripped_text
        if not character.isalnum() and not character.isspace()
    )

    if cjk_count > 0:
        if visible_count == 1 and not _is_whitelisted_short_cjk_fragment(stripped_text):
            return True
        if symbol_count >= max(1, visible_count // 2) and cjk_count < visible_count:
            return True
        return False
    if visible_count <= 1:
        return True
    if latin_count > 0 and visible_count <= 3:
        return True
    if digit_count > 0 and visible_count <= 2:
        return True
    if symbol_count >= max(1, visible_count // 2):
        return True
    return False


def _is_similar_line(a: str, b: str) -> bool:
    if a == b:
        return True

    a_key = _line_similarity_key(a)
    b_key = _line_similarity_key(b)
    if a_key and b_key:
        if a_key == b_key:
            return True
        shorter_length = min(len(a_key), len(b_key))
        longer_length = max(len(a_key), len(b_key))
        if shorter_length >= 5 and (a_key in b_key or b_key in a_key):
            return True

        if shorter_length >= 8 and longer_length / max(1, shorter_length) >= 2.2:
            return False

        key_similarity = SequenceMatcher(None, a_key, b_key).ratio()
        a_cjk, a_visible = _text_quality_score(a)
        b_cjk, b_visible = _text_quality_score(b)
        cjk_heavy = (
            a_cjk / max(1, a_visible) >= 0.55 and b_cjk / max(1, b_visible) >= 0.55
        )
        if cjk_heavy and shorter_length >= 6 and key_similarity >= 0.80:
            return True
        if key_similarity >= 0.90:
            return True

    return SequenceMatcher(None, a, b).ratio() >= 0.90


def _clean_ocr_lines(lines: list[str]) -> list[str]:
    cleaned_lines: list[str] = []
    bucket_to_cleaned_lines: dict[int, list[str]] = {}
    seen_exact: set[str] = set()
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line or _is_noise_line(stripped_line):
            continue
        if stripped_line in seen_exact:
            continue
        candidates = _iter_similarity_bucket_candidates(
            bucket_to_cleaned_lines,
            stripped_line,
        )
        if any(_is_similar_line(stripped_line, existing) for existing in candidates):
            continue
        cleaned_lines.append(stripped_line)
        bucket = _line_similarity_bucket(stripped_line)
        bucket_to_cleaned_lines.setdefault(bucket, []).append(stripped_line)
        seen_exact.add(stripped_line)
    return cleaned_lines


def _is_subsumed_line(a: str, b: str) -> bool:
    a_key = _line_similarity_key(a)
    b_key = _line_similarity_key(b)
    if not a_key or not b_key:
        return False
    if a_key == b_key:
        return True
    if min(len(a_key), len(b_key)) < 5:
        return False
    return a_key in b_key or b_key in a_key


def _is_same_line_region(a: _OcrLineMetadata, b: _OcrLineMetadata) -> bool:
    ax, ay, aw, ah = a["bbox"]
    bx, by, bw, bh = b["bbox"]
    x_tolerance = max(8.0, min(aw, bw) * 0.5)
    y_tolerance = max(10.0, max(ah, bh) * 0.6)
    return abs(ax - bx) <= x_tolerance and abs(ay - by) <= y_tolerance


def _prefer_ocr_line_metadata(a: _OcrLineMetadata, b: _OcrLineMetadata) -> _OcrLineMetadata:
    if a["conf"] != b["conf"]:
        return a if a["conf"] > b["conf"] else b

    a_cjk, a_visible = _text_quality_score(a["text"])
    b_cjk, b_visible = _text_quality_score(b["text"])
    if (a_cjk, a_visible) != (b_cjk, b_visible):
        return a if (a_cjk, a_visible) > (b_cjk, b_visible) else b

    if len(a["text"]) != len(b["text"]):
        return a if len(a["text"]) > len(b["text"]) else b

    return a if a["text"] <= b["text"] else b


def _cluster_ocr_line_metadata(lines: list[_OcrLineMetadata]) -> list[_OcrLineMetadata]:
    if not lines:
        return []

    stage1_clusters: list[_OcrLineMetadata] = []
    for line in lines:
        merged = False
        for cluster_index, existing in enumerate(stage1_clusters):
            if not _is_same_line_region(line, existing):
                continue
            if line["text"] == existing["text"] or _is_subsumed_line(line["text"], existing["text"]):
                stage1_clusters[cluster_index] = _prefer_ocr_line_metadata(existing, line)
                merged = True
                break
        if not merged:
            stage1_clusters.append(line)

    stage2_clusters: list[_OcrLineMetadata] = []
    for line in stage1_clusters:
        merged = False
        for cluster_index, existing in enumerate(stage2_clusters):
            if not _is_same_line_region(line, existing):
                continue
            if _is_similar_line(line["text"], existing["text"]):
                stage2_clusters[cluster_index] = _prefer_ocr_line_metadata(existing, line)
                merged = True
                break
        if not merged:
            stage2_clusters.append(line)

    return stage2_clusters


@lru_cache(maxsize=4096)
def _compute_page_quality_score_cached(normalized_text: str) -> float:
    if not normalized_text:
        return 0.0

    page_lines = [line.strip() for line in normalized_text.splitlines() if line.strip()]
    if not page_lines:
        return 0.0

    cleaned_lines = _clean_ocr_lines(page_lines)
    cleaned_text = "\n".join(cleaned_lines)
    cjk_count, visible_count = _text_quality_score(cleaned_text)
    latin_count = sum(1 for character in cleaned_text if "a" <= character.lower() <= "z")

    line_count = len(page_lines)
    noise_count = sum(1 for line in page_lines if _is_noise_line(line))
    deduped_non_noise_lines: list[str] = []
    deduped_bucket_to_lines: dict[int, list[str]] = {}
    duplicate_count = 0
    for line in page_lines:
        if _is_noise_line(line):
            continue
        dedupe_candidates = _iter_similarity_bucket_candidates(
            deduped_bucket_to_lines,
            line,
        )
        if any(_is_similar_line(line, existing) for existing in dedupe_candidates):
            duplicate_count += 1
            continue
        deduped_non_noise_lines.append(line)
        bucket = _line_similarity_bucket(line)
        deduped_bucket_to_lines.setdefault(bucket, []).append(line)

    confidence_ratio = min(
        1.0,
        visible_count / max(1, OCR_PAGE_SCORE_CONFIDENCE_VISIBLE_TARGET),
    )
    confidence_score = confidence_ratio * _calculate_printable_ratio(cleaned_text)
    cjk_ratio = cjk_count / max(1, visible_count)
    noise_penalty = noise_count / max(1, line_count)
    duplicate_penalty = duplicate_count / max(1, line_count)
    lexical_noise_penalty = 0.0
    if cjk_count == 0 and visible_count > 0:
        tokens = [
            "".join(character for character in line if character.isalnum())
            for line in page_lines
        ]
        normalized_tokens = [token for token in tokens if token]
        if normalized_tokens:
            short_token_ratio = sum(1 for token in normalized_tokens if len(token) <= 2) / len(
                normalized_tokens
            )
            lexical_noise_penalty = short_token_ratio * 0.35
    elif visible_count > 0 and latin_count > cjk_count:
        latin_ratio = latin_count / visible_count
        cjk_ratio_for_penalty = cjk_count / visible_count
        lexical_noise_penalty += max(0.0, latin_ratio - cjk_ratio_for_penalty) * 0.45

    thresholds = _resolve_ocr_quality_thresholds()
    quality_score = (
        confidence_score * OCR_PAGE_SCORE_CONFIDENCE_WEIGHT
        + cjk_ratio * OCR_PAGE_SCORE_CJK_RATIO_WEIGHT
        - noise_penalty * OCR_PAGE_SCORE_NOISE_PENALTY_WEIGHT
        - duplicate_penalty * OCR_PAGE_SCORE_DUPLICATE_PENALTY_WEIGHT
        - lexical_noise_penalty
    )

    noise_threshold = thresholds["noise_threshold"]
    if noise_penalty > noise_threshold:
        quality_score -= (noise_penalty - noise_threshold) * OCR_PAGE_SCORE_NOISE_PENALTY_WEIGHT

    return round(quality_score, 6)


def _compute_page_quality_score(text: str) -> float:
    normalized_text = _normalize_page_text(text)
    return _compute_page_quality_score_cached(normalized_text)


def _should_replace_page_with_ocr(
    *,
    baseline_text: str,
    ocr_text: str,
    quality_thresholds: dict[str, float] | None = None,
) -> bool:
    normalized_ocr_text = _normalize_page_text(ocr_text)
    if not normalized_ocr_text:
        return False

    thresholds = _resolve_ocr_quality_thresholds(quality_thresholds)
    ocr_score = _compute_page_quality_score(normalized_ocr_text)
    if not _normalize_page_text(baseline_text):
        return ocr_score >= thresholds["fallback_floor"]

    baseline_score = _compute_page_quality_score(baseline_text)
    return ocr_score >= baseline_score + thresholds["replacement_margin"]


def _resolve_ocr_quality_thresholds(
    overrides: dict[str, float] | None = None,
) -> dict[str, float]:
    defaults: dict[str, float] = {
        "replacement_margin": OCR_PAGE_SCORE_REPLACEMENT_MARGIN,
        "noise_threshold": OCR_PAGE_SCORE_NOISE_THRESHOLD,
        "fallback_floor": OCR_PAGE_SCORE_FALLBACK_FLOOR,
    }
    if overrides is None:
        return defaults

    resolved = dict(defaults)
    for key, value in overrides.items():
        if key not in resolved:
            raise ValueError(f"Unsupported OCR quality threshold key: {key}")
        if not isinstance(value, (int, float)):
            raise ValueError(f"Invalid OCR quality threshold `{key}`: non-numeric value")
        float_value = float(value)
        if float_value < 0.0 or float_value > 1.0:
            raise ValueError(
                f"Invalid OCR quality threshold `{key}`: expected value in [0.0, 1.0]"
            )
        resolved[key] = float_value
    return resolved


def _resolve_ocr_quality_threshold_profile(
    *,
    backend: str,
    layout_mode: str,
) -> dict[str, float]:
    engine_overrides = _OCR_THRESHOLD_PROFILE_OVERRIDES.get(backend, {})
    layout_overrides = _OCR_LAYOUT_THRESHOLD_OVERRIDES.get(layout_mode, {})
    merged_overrides = dict(engine_overrides)
    merged_overrides.update(layout_overrides)
    return _resolve_ocr_quality_thresholds(merged_overrides)


def _resolve_key_content_min_page_score(
    *,
    layout_mode: str,
) -> float:
    min_page_score = OCR_KEY_CONTENT_MIN_PAGE_SCORE
    if layout_mode == OCR_LAYOUT_VERTICAL:
        min_page_score -= 0.02
    return max(0.10, min(0.50, min_page_score))


def _apply_phrase_corrections(
    text: str,
    corrections: tuple[tuple[str, str], ...],
) -> tuple[str, int, list[str]]:
    punctuation_boundaries = set(
        " \t\n\r,.!?;:'\"()[]{}<>/\\|-_=+*&^%$#@~`，。！？；：「」『』（）《》〈〉【】〔〕、"
    )

    def _is_cjk_phrase(value: str) -> bool:
        normalized = value.replace("\n", "").strip()
        if not normalized:
            return False
        return all("\u4e00" <= character <= "\u9fff" for character in normalized)

    def _is_boundary_character(value: str) -> bool:
        return value in punctuation_boundaries

    def _replace_with_boundary_guard(
        source_text: str,
        source_phrase: str,
        target_phrase: str,
    ) -> tuple[str, int]:
        if len(source_phrase.replace("\n", "").strip()) > 3:
            occurrence_count = source_text.count(source_phrase)
            if occurrence_count <= 0:
                return source_text, 0
            return source_text.replace(source_phrase, target_phrase), occurrence_count

        guarded_replacement_count = 0
        chunks: list[str] = []
        cursor = 0
        source_length = len(source_phrase)
        while True:
            hit_index = source_text.find(source_phrase, cursor)
            if hit_index < 0:
                chunks.append(source_text[cursor:])
                break

            left_boundary_index = hit_index - 1
            right_boundary_index = hit_index + source_length
            left_character = (
                source_text[left_boundary_index] if left_boundary_index >= 0 else ""
            )
            right_character = (
                source_text[right_boundary_index]
                if right_boundary_index < len(source_text)
                else ""
            )
            left_ok = left_boundary_index < 0 or _is_boundary_character(left_character)
            right_ok = right_boundary_index >= len(source_text) or _is_boundary_character(right_character)

            chunks.append(source_text[cursor:hit_index])
            if left_ok and right_ok:
                chunks.append(target_phrase)
                guarded_replacement_count += 1
            else:
                chunks.append(source_phrase)
            cursor = hit_index + source_length

        return "".join(chunks), guarded_replacement_count

    corrected = text
    replacement_count = 0
    examples: list[str] = []
    for source, target in corrections:
        if _is_cjk_phrase(source):
            occurrence_count = corrected.count(source)
            if occurrence_count > 0:
                updated_text = corrected.replace(source, target)
            else:
                updated_text = corrected
        else:
            updated_text, occurrence_count = _replace_with_boundary_guard(corrected, source, target)
        if occurrence_count <= 0:
            continue
        corrected = updated_text
        replacement_count += occurrence_count
        if len(examples) < 3:
            examples.append(f"{source}->{target}")
    return corrected, replacement_count, examples


def _is_low_confidence_page_text(text: str) -> bool:
    normalized_text = _normalize_page_text(text)
    if not normalized_text:
        return True
    if _is_weak_page_text(normalized_text):
        return True
    return _compute_page_quality_score(normalized_text) < OCR_CLASSICAL_ZH_AGGRESSIVE_SCORE_THRESHOLD


def _apply_classical_zh_postprocess(page_texts: list[str]) -> tuple[list[str], str]:
    corrected_pages: list[str] = []
    safe_replacements = 0
    aggressive_replacements = 0
    aggressive_pages = 0
    safe_examples: list[str] = []
    aggressive_examples: list[str] = []

    for page_number, page_text in enumerate(page_texts, start=1):
        corrected, safe_count, safe_page_examples = _apply_phrase_corrections(
            page_text,
            CLASSICAL_ZH_PHRASE_CORRECTIONS,
        )
        safe_replacements += safe_count
        if safe_page_examples and len(safe_examples) < 3:
            safe_examples.append(f"p{page_number}:{safe_page_examples[0]}")

        if _is_low_confidence_page_text(corrected):
            corrected, aggressive_count, aggressive_page_examples = _apply_phrase_corrections(
                corrected,
                CLASSICAL_ZH_AGGRESSIVE_CORRECTIONS,
            )
            if aggressive_count > 0:
                aggressive_pages += 1
                aggressive_replacements += aggressive_count
                if aggressive_page_examples and len(aggressive_examples) < 3:
                    aggressive_examples.append(f"p{page_number}:{aggressive_page_examples[0]}")

        corrected_pages.append(_normalize_page_text(corrected))

    diagnostics = (
        "classical_zh_postprocess: "
        f"safe_replacements={safe_replacements} "
        f"aggressive_replacements={aggressive_replacements} "
        f"aggressive_pages={aggressive_pages} "
        f"safe_examples={'|'.join(safe_examples) if safe_examples else '-'} "
        f"aggressive_examples={'|'.join(aggressive_examples) if aggressive_examples else '-'}"
    )
    return corrected_pages, diagnostics


def _build_key_content_lines(text: str, *, max_lines: int = OCR_KEY_CONTENT_MAX_LINES) -> list[str]:
    normalized_text = _normalize_page_text(text)
    if not normalized_text:
        return []
    lines = [line.strip() for line in normalized_text.splitlines() if line.strip()]
    cleaned_lines = _clean_ocr_lines(lines)
    return cleaned_lines[: max(1, max_lines)]


def _should_use_key_content_fallback(
    *,
    baseline_text: str,
    ocr_text: str,
    min_page_score: float = OCR_KEY_CONTENT_MIN_PAGE_SCORE,
) -> bool:
    if not _is_low_confidence_page_text(baseline_text):
        return False
    if _compute_page_quality_score(ocr_text) < min_page_score:
        return False
    return bool(_build_key_content_lines(ocr_text))


def _render_key_content_fallback_page(*, page_number: int, source_text: str) -> str:
    key_lines = _build_key_content_lines(source_text)
    if not key_lines:
        return ""
    return "\n".join(
        [f"[KEY-CONTENT FALLBACK page={page_number}]", *key_lines]
    )


def _extract_page_raw_texts(
    input_pdf: Path,
    *,
    page_count: int | None = None,
    max_pages: int | None = None,
    progress_callback: _PageProgressCallback | None = None,
) -> list[str]:
    high_level = importlib.import_module("pdfminer.high_level")
    layout = importlib.import_module("pdfminer.layout")
    extract_pages = cast(_ExtractPages, getattr(high_level, "extract_pages"))
    lt_text_container = cast(type[object], getattr(layout, "LTTextContainer"))

    resolved_page_count = page_count if page_count and page_count > 0 else _extract_page_count(input_pdf)
    if max_pages is not None:
        resolved_page_count = min(resolved_page_count, max(1, max_pages))
    page_layouts = extract_pages(str(input_pdf), maxpages=resolved_page_count)
    page_texts: list[str] = []
    for page_number, page_layout in enumerate(page_layouts, start=1):
        buffer = io.StringIO()
        for element in page_layout:
            if isinstance(element, lt_text_container):
                _ = buffer.write(cast(_HasGetText, element).get_text())
        page_texts.append(buffer.getvalue())
        if progress_callback is not None:
            progress_callback(page_number, resolved_page_count)

    return page_texts


def _extract_page_count(input_pdf: Path) -> int:
    pypdf = importlib.import_module("pypdf")
    pdf_reader_cls = cast(Any, getattr(pypdf, "PdfReader"))
    pdf_reader = cast(Any, pdf_reader_cls(str(input_pdf)))
    pages = cast(list[object], getattr(pdf_reader, "pages"))
    return max(1, len(pages))


def _iter_page_windows(page_count: int, window_size: int) -> Iterator[tuple[int, int]]:
    bounded_page_count = max(1, page_count)
    bounded_window_size = max(1, min(window_size, OCR_PAGE_WINDOW_SIZE))
    for start_index in range(1, bounded_page_count + 1, bounded_window_size):
        end_index = min(bounded_page_count, start_index + bounded_window_size - 1)
        yield (start_index, end_index)


def _iter_selected_page_windows(
    page_indices: list[int], window_size: int
) -> Iterator[tuple[int, int]]:
    if not page_indices:
        return
    sorted_indices = sorted(set(index for index in page_indices if index >= 0))
    bounded_window_size = max(1, min(window_size, OCR_PAGE_WINDOW_SIZE))

    run_start = sorted_indices[0]
    run_end = sorted_indices[0]

    def _yield_chunked_windows(start_index: int, end_index: int) -> Iterator[tuple[int, int]]:
        current = start_index
        while current <= end_index:
            chunk_end = min(end_index, current + bounded_window_size - 1)
            yield (current + 1, chunk_end + 1)
            current = chunk_end + 1

    for page_index in sorted_indices[1:]:
        if page_index == run_end + 1:
            run_end = page_index
            continue
        yield from _yield_chunked_windows(run_start, run_end)
        run_start = page_index
        run_end = page_index

    yield from _yield_chunked_windows(run_start, run_end)


class _OcrRuntimeTuningProfile(TypedDict):
    onnx_intra_op_threads: int
    onnx_inter_op_threads: int
    pdf2image_thread_count: int


def _resolve_ocr_runtime_tuning_profile(*, extraction_workers: int) -> _OcrRuntimeTuningProfile:
    cpu_count = os.cpu_count() or 1
    if extraction_workers <= 1:
        return {
            "onnx_intra_op_threads": max(1, min(4, cpu_count // 2 if cpu_count > 1 else 1)),
            "onnx_inter_op_threads": 1,
            "pdf2image_thread_count": max(
                1,
                min(cpu_count, OCR_PDF2IMAGE_THREAD_COUNT_SINGLE_WORKER_CAP),
            ),
        }

    per_worker_cpu_budget = max(1, cpu_count // extraction_workers)
    onnx_intra = max(1, min(2, per_worker_cpu_budget))
    return {
        "onnx_intra_op_threads": onnx_intra,
        "onnx_inter_op_threads": 1,
        "pdf2image_thread_count": 1,
    }


def _build_rapidocr_runtime_params(
    *,
    tuning_profile: _OcrRuntimeTuningProfile,
) -> dict[str, object]:
    return {
        "EngineConfig.onnxruntime.intra_op_num_threads": tuning_profile[
            "onnx_intra_op_threads"
        ],
        "EngineConfig.onnxruntime.inter_op_num_threads": tuning_profile[
            "onnx_inter_op_threads"
        ],
    }


def _build_rapidocr_ocr_extractor(
    layout_mode: str = OCR_LAYOUT_AUTO,
    *,
    tuning_profile: _OcrRuntimeTuningProfile,
) -> _ImageToString:
    rapidocr_module = importlib.import_module("rapidocr_onnxruntime")
    rapidocr_cls = cast(Any, getattr(rapidocr_module, "RapidOCR"))
    rapidocr_params = _build_rapidocr_runtime_params(tuning_profile=tuning_profile)
    rapidocr_engine = cast(object, rapidocr_cls(params=rapidocr_params))

    def _extract_box_metrics(box: object) -> tuple[float, float, float, float] | None:
        if not isinstance(box, list) or len(box) < 4:
            return None
        points: list[tuple[float, float]] = []
        for point in box[:4]:
            if not isinstance(point, list) or len(point) < 2:
                return None
            x_val = point[0]
            y_val = point[1]
            if not isinstance(x_val, (int, float)) or not isinstance(y_val, (int, float)):
                return None
            points.append((float(x_val), float(y_val)))
        x_values = [point[0] for point in points]
        y_values = [point[1] for point in points]
        min_x = min(x_values)
        max_x = max(x_values)
        min_y = min(y_values)
        max_y = max(y_values)
        center_x = (min_x + max_x) / 2.0
        center_y = (min_y + max_y) / 2.0
        width = max_x - min_x
        height = max_y - min_y
        return center_x, center_y, width, height

    def _order_entries(entries: list[tuple[str, float, float, float, float]]) -> list[str]:
        if layout_mode == OCR_LAYOUT_VERTICAL:
            ordered_entries = sorted(
                entries,
                key=lambda item: (-item[1], item[2], item[0], item[3], item[4]),
            )
        else:
            ordered_entries = sorted(
                entries,
                key=lambda item: (item[2], item[1], item[0], item[3], item[4]),
            )
        return [text for text, _, _, _, _ in ordered_entries if text]

    def _extract(image: object) -> str:
        ocr_callable = getattr(rapidocr_engine, "__call__", None)
        if not callable(ocr_callable):
            return ""

        try:
            ocr_result = cast(object, ocr_callable(image))
        except Exception:
            return ""

        if not isinstance(ocr_result, tuple) or not ocr_result:
            return ""
        line_items = ocr_result[0]
        if not isinstance(line_items, list):
            return ""

        normalized_lines: list[_OcrLineMetadata] = []
        for item in line_items:
            if not isinstance(item, list) or len(item) < 3:
                continue
            metrics = _extract_box_metrics(item[0])
            if metrics is None:
                continue
            text_value = item[1]
            confidence_value = item[2]
            if not isinstance(text_value, str):
                continue
            text = text_value.strip()
            if not text:
                continue
            confidence = float(confidence_value) if isinstance(confidence_value, (int, float)) else 0.0
            normalized_lines.append(
                {
                    "text": text,
                    "conf": confidence,
                    "bbox": metrics,
                    "engine": "rapidocr",
                    "variant": "default",
                }
            )

        if not normalized_lines:
            return ""

        confidence_floor = _resolve_ocr_quality_thresholds()["fallback_floor"]
        floor_lines = [line for line in normalized_lines if line["conf"] >= confidence_floor]
        if not floor_lines:
            return ""

        high_conf_lines = [line for line in floor_lines if line["conf"] >= OCR_MIN_CONFIDENCE]
        high_conf_ratio = len(high_conf_lines) / len(floor_lines)
        selected_lines = (
            high_conf_lines if high_conf_lines and high_conf_ratio >= OCR_HIGH_CONF_MIN_RATIO else floor_lines
        )
        clustered_lines = _cluster_ocr_line_metadata(selected_lines)
        ordered_lines = _order_entries([(line["text"], *line["bbox"]) for line in clustered_lines])
        return "\n".join(_clean_ocr_lines(ordered_lines))

    return _extract


def _resolve_pdf2image_thread_count(*, tuning_profile: _OcrRuntimeTuningProfile) -> int:
    return max(1, tuning_profile["pdf2image_thread_count"])


def _extract_page_raw_texts_with_ocr(
    input_pdf: Path,
    *,
    page_count: int | None = None,
    backend: str = OCR_DEFAULT_ENGINE,
    layout_mode: str = OCR_LAYOUT_AUTO,
    page_indices: list[int] | None = None,
    progress_callback: _PageProgressCallback | None = None,
    workers: int | None = None,
    resource_guard_timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    resource_guard_fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
) -> list[str]:
    if backend != OCR_DEFAULT_ENGINE:
        raise RuntimeError(_unimplemented_backend_message(backend))

    pdf2image = importlib.import_module("pdf2image")
    convert_from_path = cast(_ConvertFromPath, getattr(pdf2image, "convert_from_path"))

    if page_indices:
        windows = list(_iter_selected_page_windows(page_indices, OCR_PAGE_WINDOW_SIZE))
    else:
        resolved_page_count = (
            page_count if page_count and page_count > 0 else _extract_page_count(input_pdf)
        )
        windows = list(_iter_page_windows(resolved_page_count, OCR_PAGE_WINDOW_SIZE))

    extraction_workers = _resolve_ocr_extraction_workers(
        window_count=len(windows), requested_workers=workers
    )
    tuning_profile = _resolve_ocr_runtime_tuning_profile(
        extraction_workers=extraction_workers,
    )
    cache_key = (
        OCR_DEFAULT_ENGINE,
        layout_mode,
        (
            f"intra={tuning_profile['onnx_intra_op_threads']}|"
            f"inter={tuning_profile['onnx_inter_op_threads']}"
        ),
    )
    image_to_string: _ImageToString | None = None
    if _ocr_extractor_cache_enabled:
        image_to_string = _OCR_EXTRACTOR_CACHE.get(cache_key)

    if image_to_string is None:
        image_to_string = _build_rapidocr_ocr_extractor(
            layout_mode=layout_mode,
            tuning_profile=tuning_profile,
        )
        if _ocr_extractor_cache_enabled:
            _OCR_EXTRACTOR_CACHE[cache_key] = image_to_string
    pdf2image_thread_count = _resolve_pdf2image_thread_count(
        tuning_profile=tuning_profile,
    )
    if image_to_string is None:
        raise RuntimeError("OCR extractor initialization failed")
    resolved_image_to_string = image_to_string

    page_text_by_number: dict[int, str] = {}
    progress_total = len(page_indices) if page_indices else max(1, sum(last - first + 1 for first, last in windows))

    def _prepare_image_for_weak_page_preset(*, image: object, dpi: int) -> object:
        if dpi <= OCR_MAX_DPI:
            return image

        pil_image_ops = importlib.import_module("PIL.ImageOps")
        autocontrast = cast(Callable[[object], object], getattr(pil_image_ops, "autocontrast"))

        convert_method = getattr(image, "convert", None)
        if not callable(convert_method):
            return image

        grayscale = cast(object, convert_method("L"))
        contrasted = cast(object, autocontrast(grayscale))
        if dpi < 300:
            return contrasted

        point_method = getattr(contrasted, "point", None)
        if not callable(point_method):
            return contrasted
        return cast(object, point_method(lambda pixel: 255 if pixel > 170 else 0))

    def _process_window(first_page: int, last_page: int) -> list[tuple[int, str]]:
        _wait_for_resource_headroom(
            timeout_seconds=resource_guard_timeout_seconds,
            fail_open=resource_guard_fail_open,
        )
        expected_count = last_page - first_page + 1
        page_best_text: dict[int, str] = {}
        page_best_score: dict[int, float] = {}

        if page_indices:
            render_dpis = OCR_WEAK_PAGE_PRESET_DPIS
            weak_page_score_threshold = _resolve_key_content_min_page_score(
                layout_mode=layout_mode,
            )
        else:
            render_dpis = (OCR_MAX_DPI,)
            weak_page_score_threshold = 0.0

        for dpi_index, dpi in enumerate(render_dpis):
            if dpi_index > 0 and page_indices:
                should_retry = any(
                    page_best_score.get(page_number, 0.0) < weak_page_score_threshold
                    for page_number in range(first_page, last_page + 1)
                )
                if not should_retry:
                    break
            try:
                images = convert_from_path(
                    str(input_pdf),
                    first_page=first_page,
                    last_page=last_page,
                    dpi=dpi,
                    size=OCR_MAX_IMAGE_SIZE,
                    thread_count=pdf2image_thread_count,
                    grayscale=OCR_PDF2IMAGE_USE_GRAYSCALE,
                    use_pdftocairo=OCR_PDF2IMAGE_USE_PDFTOCAIRO,
                    timeout=OCR_PDF2IMAGE_TIMEOUT_SECONDS,
                )
            except TypeError:
                images = convert_from_path(
                    str(input_pdf),
                    first_page=first_page,
                    last_page=last_page,
                    dpi=dpi,
                    size=OCR_MAX_IMAGE_SIZE,
                )

            for image_offset in range(expected_count):
                page_number = first_page + image_offset
                if image_offset >= len(images):
                    candidate_text = ""
                else:
                    candidate_image = _prepare_image_for_weak_page_preset(
                        image=images[image_offset],
                        dpi=dpi,
                    )
                    candidate_text = resolved_image_to_string(candidate_image)

                candidate_score = _compute_page_quality_score(candidate_text)
                current_best_score = page_best_score.get(page_number)
                if current_best_score is None or candidate_score > current_best_score:
                    page_best_score[page_number] = candidate_score
                    page_best_text[page_number] = candidate_text

        window_results = [
            (page_number, page_best_text.get(page_number, ""))
            for page_number in range(first_page, last_page + 1)
        ]
        return window_results

    progress_current = 0
    if windows:
        _wait_for_resource_headroom(
            timeout_seconds=resource_guard_timeout_seconds,
            fail_open=resource_guard_fail_open,
        )
        with ThreadPoolExecutor(max_workers=extraction_workers) as executor:
            future_map = {
                executor.submit(_process_window, first_page, last_page): (first_page, last_page)
                for first_page, last_page in windows
            }
            for future in as_completed(future_map):
                window_results = future.result()
                for page_number, page_text in window_results:
                    page_text_by_number[page_number] = page_text
                    progress_current += 1
                    if progress_callback is not None:
                        progress_callback(progress_current, progress_total)

    if page_indices:
        return [page_text_by_number.get(page_index + 1, "") for page_index in page_indices]

    if not windows:
        return [""]
    max_page_number = max(last_page for _, last_page in windows)
    return [page_text_by_number.get(page_number, "") for page_number in range(1, max_page_number + 1)]


def _resolve_ocr_extraction_workers(*, window_count: int, requested_workers: int | None = None) -> int:
    if window_count <= 0:
        return 1
    return _resolve_parallel_workers(window_count, requested_workers=requested_workers)


def _extract_page_raw_texts_with_pdfplumber(
    input_pdf: Path, page_indices: list[int]
) -> dict[int, str]:
    pdfplumber = importlib.import_module("pdfplumber")
    pdfplumber_open = cast(_PdfPlumberOpen, getattr(pdfplumber, "open"))

    if not page_indices:
        return {}

    requested_indices = set(page_indices)
    page_texts: dict[int, str] = {}
    with pdfplumber_open(str(input_pdf)) as pdf_document:
        for page_index in sorted(requested_indices):
            if page_index < 0 or page_index >= len(pdf_document.pages):
                continue
            page = pdf_document.pages[page_index]
            extracted_text = page.extract_text(layout=True)
            page_texts[page_index] = extracted_text or ""
    return page_texts


def get_optional_backend_availability() -> dict[str, bool]:
    availability: dict[str, bool] = {}
    for backend in OPTIONAL_BACKENDS:
        module_names = _OPTIONAL_BACKEND_MODULES[backend]
        availability[backend] = all(
            importlib.util.find_spec(module_name) is not None
            for module_name in module_names
        )
    return availability


def get_optional_backend_missing_dependency_message(backend: str) -> str:
    guidance = _OPTIONAL_BACKEND_GUIDANCE.get(backend)
    if guidance is None:
        return f"Unknown backend `{backend}`."
    return (
        f"Selected OCR backend `{backend}` is unavailable. "
        f"{guidance}"
    )


def _write_stderr_line(message: str) -> None:
    _ = sys.stderr.write(f"{message}\n")


def _build_progress_event(percent: int, stage: str) -> _ProgressEvent:
    bounded_percent = max(0, min(100, percent))
    return {"percent": bounded_percent, "stage": stage}


def _render_progress_line(event: _ProgressEvent, *, progress_format: str = PROGRESS_FORMAT_TEXT) -> str:
    if progress_format == PROGRESS_FORMAT_TEXT:
        return f"Progress: {event['percent']}% {event['stage']}"
    if progress_format == PROGRESS_FORMAT_JSONL:
        return json.dumps(event, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    raise ValueError(f"Unsupported progress format: {progress_format}")


def _write_progress(percent: int, stage: str, *, progress_format: str | None = None) -> None:
    event = _build_progress_event(percent, stage)
    resolved_progress_format = progress_format or _active_progress_format
    _write_stderr_line(_render_progress_line(event, progress_format=resolved_progress_format))


def _build_page_progress_writer(
    *,
    stage_label: str,
    range_start: int,
    range_end: int,
) -> _PageProgressCallback:
    bounded_range_start = max(0, min(100, range_start))
    bounded_range_end = max(bounded_range_start, min(100, range_end))
    _ = bounded_range_end

    emitted_stage_start = False

    def _write_page_progress(current: int, total: int) -> None:
        nonlocal emitted_stage_start
        bounded_total = max(1, total)
        bounded_current = max(0, min(current, bounded_total))

        if not emitted_stage_start:
            emitted_stage_start = True
            _write_progress(
                0,
                (
                    f"{stage_label} current=0 total={bounded_total} "
                    f"remaining={bounded_total} page_percent=0%"
                ),
            )

        if bounded_current == 0:
            return

        remaining = max(0, bounded_total - bounded_current)
        page_percent = int((bounded_current * 100) / bounded_total)
        _write_progress(
            page_percent,
            (
                f"{stage_label} current={bounded_current} total={bounded_total} "
                f"remaining={remaining} page_percent={page_percent}%"
            ),
        )

    return _write_page_progress


def _classify_pdf_failure_kind(input_pdf: Path, error: Exception) -> str | None:
    file_name = input_pdf.name.lower()
    error_text = str(error).lower()

    if "encrypted" in file_name:
        return "encrypted"
    if "corrupt" in file_name:
        return "corrupt"

    encrypted_hints = ("encrypted", "password", "decrypt", "/encrypt")
    if any(hint in error_text for hint in encrypted_hints):
        return "encrypted"

    corrupt_hints = (
        "no /root object",
        "is this really a pdf",
        "eof marker",
        "malformed",
        "xref",
        "trailer",
    )
    if any(hint in error_text for hint in corrupt_hints):
        return "corrupt"

    return None


def _conversion_failed_message(error: Exception, *, input_pdf: Path | None = None) -> str:
    if input_pdf is not None:
        failure_kind = _classify_pdf_failure_kind(input_pdf, error)
        if failure_kind == "encrypted":
            return (
                "Conversion failed: Encrypted PDF detected. "
                "Decrypt the PDF with the correct password and retry."
            )
        if failure_kind == "corrupt":
            return (
                "Conversion failed: Corrupt PDF detected. "
                "Re-export or repair the PDF file, then retry."
            )
    return f"Conversion failed: {error}"


def _missing_ocr_dependency_message(backend: str, missing_module: str | None) -> str:
    module_name = missing_module or "unknown module"
    install_hint = _DEPENDENCY_INSTALL_HINT_BY_ENGINE.get(
        backend,
        "required OCR dependencies",
    )
    return (
        "OCR fallback requested but dependency is missing: "
        f"{module_name}. Install {install_hint}."
    )


def _auto_ocr_skip_message(reason: str) -> str:
    return (
        "OCR auto mode could not run OCR and continued without OCR: "
        f"{reason}"
    )


def _zh_script_dependency_missing_message(target_script: str) -> str:
    return (
        "Chinese script conversion requested but dependency is missing: "
        f"opencc (target={target_script}). Install `OpenCC`."
    )


def _no_extractable_text_warning(ocr_fallback_enabled: bool) -> str:
    if ocr_fallback_enabled:
        return (
            "Warning: OCR fallback completed but no extractable text found; "
            "output contains page headers only."
        )
    return (
        "Warning: no extractable text found in PDF pages; "
        "output contains page headers only."
    )


def _ocr_diagnostics_message(
    *,
    requested_engine: str,
    weak_pages_before_pdfplumber: int,
    weak_pages_after_pdfplumber: int,
    ocr_pages_requested: int,
    ocr_pages_applied: int,
    ocr_retry_count: int = 0,
    resolved_engine: str | None = None,
    dependency_missing_module: str | None = None,
    backend_error: str | None = None,
) -> str:
    parts = [
        "Diagnostics:",
        "mode=ocr_fallback",
        f"requested_engine={requested_engine}",
        f"weak_pages_before_pdfplumber={weak_pages_before_pdfplumber}",
        f"weak_pages_after_pdfplumber={weak_pages_after_pdfplumber}",
        f"ocr_pages_requested={ocr_pages_requested}",
        f"ocr_pages_applied={ocr_pages_applied}",
        f"ocr_retry_count={ocr_retry_count}",
    ]
    if resolved_engine and resolved_engine != requested_engine:
        parts.append(f"resolved_engine={resolved_engine}")
    if dependency_missing_module:
        parts.append(f"missing_dependency={dependency_missing_module}")
    if backend_error:
        parts.append(f"backend_error={backend_error}")
    return " ".join(parts)


def _pdfplumber_fallback_warning(error: Exception) -> str:
    return (
        "Diagnostics: mode=layout_assist stage=pdfplumber "
        f"status=skipped error_type={error.__class__.__name__}"
    )


def _unimplemented_backend_message(backend: str) -> str:
    return f"Unsupported OCR backend: {backend}. Use `rapidocr`."


def _is_transient_ocr_error(error: BaseException) -> bool:
    if isinstance(error, TimeoutError):
        return True

    transient_error_numbers = {
        errno.EAGAIN,
        errno.EBUSY,
        errno.ETIMEDOUT,
        errno.ECONNRESET,
        errno.ENOBUFS,
    }
    if isinstance(error, OSError) and error.errno in transient_error_numbers:
        return True

    error_text = str(error).lower()
    transient_hints = (
        "temporarily unavailable",
        "temporary failure",
        "resource busy",
        "timed out",
        "timeout",
        "try again",
        "connection reset",
    )
    return any(hint in error_text for hint in transient_hints)


def _run_ocr_backend_with_transient_retry(
    *,
    input_pdf: Path,
    requested_engine: str,
    page_count: int,
    layout_mode: str,
    page_indices: list[int],
    progress_callback: _PageProgressCallback,
    workers: int | None,
    resource_guard_timeout_seconds: float,
    resource_guard_fail_open: bool,
) -> tuple[list[str], int]:
    retry_count = 0
    for attempt in range(1, OCR_TRANSIENT_RETRY_MAX_ATTEMPTS + 1):
        try:
            return (
                _extract_page_raw_texts_with_backend(
                    input_pdf,
                    requested_engine,
                    page_count=page_count,
                    layout_mode=layout_mode,
                    page_indices=page_indices,
                    progress_callback=progress_callback,
                    workers=workers,
                    resource_guard_timeout_seconds=resource_guard_timeout_seconds,
                    resource_guard_fail_open=resource_guard_fail_open,
                ),
                retry_count,
            )
        except (RuntimeError, OSError, TimeoutError) as error:
            if attempt >= OCR_TRANSIENT_RETRY_MAX_ATTEMPTS or not _is_transient_ocr_error(error):
                setattr(error, "_ocr_retry_count", retry_count)
                raise
            retry_count += 1
            backoff_seconds = OCR_TRANSIENT_RETRY_BACKOFF_SECONDS[
                min(retry_count - 1, len(OCR_TRANSIENT_RETRY_BACKOFF_SECONDS) - 1)
            ]
            _write_stderr_line(
                f"OCR transient failure detected; retrying attempt={attempt + 1}/"
                f"{OCR_TRANSIENT_RETRY_MAX_ATTEMPTS} backoff={backoff_seconds:.2f}s "
                f"error={error.__class__.__name__}"
            )
            time.sleep(backoff_seconds)

    return [], retry_count


def _extract_page_raw_texts_with_backend(
    input_pdf: Path,
    backend: str,
    *,
    page_count: int | None = None,
    layout_mode: str = OCR_LAYOUT_AUTO,
    page_indices: list[int] | None = None,
    progress_callback: _PageProgressCallback | None = None,
    workers: int | None = None,
    resource_guard_timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    resource_guard_fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
) -> list[str]:
    if backend == "rapidocr":
        if progress_callback is None:
            return _extract_page_raw_texts_with_ocr(
                input_pdf,
                backend=backend,
                page_count=page_count,
                layout_mode=layout_mode,
                page_indices=page_indices,
                workers=workers,
                resource_guard_timeout_seconds=resource_guard_timeout_seconds,
                resource_guard_fail_open=resource_guard_fail_open,
            )
        return _extract_page_raw_texts_with_ocr(
            input_pdf,
            backend=backend,
            page_count=page_count,
            layout_mode=layout_mode,
            page_indices=page_indices,
            progress_callback=progress_callback,
            workers=workers,
            resource_guard_timeout_seconds=resource_guard_timeout_seconds,
            resource_guard_fail_open=resource_guard_fail_open,
        )

    raise RuntimeError(_unimplemented_backend_message(backend))


def extract_page_texts(
    input_pdf: Path,
    *,
    page_count: int | None = None,
    max_pages: int | None = None,
    progress_callback: _PageProgressCallback | None = None,
) -> list[str]:
    raw_pages = _extract_page_raw_texts(
        input_pdf,
        page_count=page_count,
        max_pages=max_pages,
        progress_callback=progress_callback,
    )
    if not raw_pages:
        return [""]
    return [_normalize_page_text(page_text) for page_text in raw_pages]


def format_markdown_pages(page_texts: list[str]) -> str:
    return "".join(format_markdown_pages_streaming(page_texts))


def format_markdown_pages_streaming(
    page_texts: Iterable[str], *, page_start: int = 1
) -> Iterator[str]:
    has_pages = False
    start_page = max(1, page_start)
    for page_offset, page_text in enumerate(page_texts):
        page_number = start_page + page_offset
        has_pages = True
        normalized_page_text = page_text.rstrip()
        if page_offset > 0:
            yield "\n\n"

        yield f"# Page {page_number}"
        if normalized_page_text:
            yield "\n\n"
            yield normalized_page_text

    if not has_pages:
        yield "\n"
        return
    yield "\n"


def write_markdown(output_path: Path, markdown_text: str) -> None:
    write_markdown_stream(output_path, [markdown_text])


def write_markdown_stream(output_path: Path, markdown_chunks: Iterable[str]) -> None:
    with output_path.open("w", encoding="utf-8") as output_file:
        for chunk in markdown_chunks:
            _ = output_file.write(chunk)


def _resolve_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_default_output_path(input_pdf: Path) -> Path:
    downloads_dir = _resolve_project_root() / _DEFAULT_DOWNLOADS_DIR_NAME / input_pdf.stem
    return downloads_dir / f"{input_pdf.stem}.md"


def _positive_int(value: str) -> int:
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed_value


def _positive_float(value: str) -> float:
    parsed_value = float(value)
    if parsed_value <= 0.0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return parsed_value


def _iter_page_chunks(
    page_texts: list[str], pages_per_chunk: int
) -> Iterator[tuple[int, int, list[str]]]:
    chunk_size = max(1, pages_per_chunk)
    page_total = len(page_texts)
    if page_total <= 0:
        yield (1, 1, [""])
        return

    for start_index in range(0, page_total, chunk_size):
        end_index = min(page_total, start_index + chunk_size)
        start_page = start_index + 1
        end_page = end_index
        yield (start_page, end_page, page_texts[start_index:end_index])


def _iter_page_ranges(total_pages: int, pages_per_chunk: int) -> Iterator[tuple[int, int]]:
    chunk_size = max(1, pages_per_chunk)
    bounded_total = max(1, total_pages)
    for start_page in range(1, bounded_total + 1, chunk_size):
        end_page = min(bounded_total, start_page + chunk_size - 1)
        yield (start_page, end_page)


def _write_pdf_page_chunk(
    *,
    source_pages: list[object],
    output_pdf: Path,
    start_page: int,
    end_page: int,
) -> None:
    pypdf = importlib.import_module("pypdf")
    pdf_writer_cls = cast(Any, getattr(pypdf, "PdfWriter"))
    pdf_writer = cast(Any, pdf_writer_cls())

    start_index = max(0, start_page - 1)
    end_index = min(len(source_pages), end_page)
    add_page = getattr(pdf_writer, "add_page", None)
    if not callable(add_page):
        raise RuntimeError("PdfWriter.add_page is unavailable.")
    for page_index in range(start_index, end_index):
        add_page(source_pages[page_index])

    write_method = getattr(pdf_writer, "write", None)
    if not callable(write_method):
        raise RuntimeError("PdfWriter.write is unavailable.")
    with output_pdf.open("wb") as output_file:
        write_method(output_file)


def _run_split_before_ocr_conversion(
    *,
    input_pdf: Path,
    output_path: Path,
    total_pages: int,
    chunk_size: int,
    force: bool,
    ocr_mode: str,
    ocr_engine: str,
    ocr_layout: str,
    ocr_classical_zh_postprocess: bool,
    ocr_key_content_fallback: bool,
    zh_script: str = ZH_SCRIPT_KEEP,
    split_ocr_parallel: bool = False,
    workers: int | None = None,
    resource_guard_timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    resource_guard_fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
    progress_format: str = PROGRESS_FORMAT_TEXT,
) -> int:
    pypdf = importlib.import_module("pypdf")
    pdf_reader_cls = cast(Any, getattr(pypdf, "PdfReader"))
    pdf_reader = cast(Any, pdf_reader_cls(str(input_pdf)))
    source_pages = cast(list[object], getattr(pdf_reader, "pages"))

    output_base = _resolve_chunk_output_base(output_path, input_pdf)
    chunk_ranges = list(_iter_page_ranges(total_pages, chunk_size))
    chunk_total = len(chunk_ranges)

    global _ocr_extractor_cache_enabled
    previous_cache_enabled = _ocr_extractor_cache_enabled
    _ocr_extractor_cache_enabled = True
    _OCR_EXTRACTOR_CACHE.clear()
    try:
        with tempfile.TemporaryDirectory(prefix="pdf_to_md_split_") as temp_dir:
            temp_root = Path(temp_dir)
            chunk_jobs: list[tuple[int, int, int, Path]] = []
            for chunk_index, (start_page, end_page) in enumerate(chunk_ranges, start=1):
                chunk_output = _build_chunk_output_path(output_base, start_page, end_page)
                if chunk_output.exists() and not force:
                    _write_stderr_line(
                        f"Output file already exists: {chunk_output}. Use --force to overwrite."
                    )
                    return 1

                split_pdf = temp_root / f"split_{chunk_index:04d}_{start_page:04d}_{end_page:04d}.pdf"
                _write_pdf_page_chunk(
                    source_pages=source_pages,
                    output_pdf=split_pdf,
                    start_page=start_page,
                    end_page=end_page,
                )
                chunk_jobs.append((chunk_index, start_page, end_page, split_pdf))

            def _run_single_chunk(*, split_pdf: Path, chunk_output: Path) -> int:
                split_ocr_fallback_enabled = ocr_mode == "strict"
                split_ocr_mode_arg = OCR_MODE_AUTO if ocr_mode == "auto" else None
                execute_kwargs: dict[str, Any] = {
                    "input_pdf_arg": str(split_pdf),
                    "output_arg": str(chunk_output),
                    "force_arg": True,
                    "ocr_fallback_arg": split_ocr_fallback_enabled,
                    "ocr_mode_arg": split_ocr_mode_arg,
                    "ocr_engine_arg": ocr_engine,
                    "ocr_layout_arg": ocr_layout,
                    "ocr_classical_zh_postprocess_arg": ocr_classical_zh_postprocess,
                    "ocr_key_content_fallback_arg": ocr_key_content_fallback,
                    "zh_script_arg": zh_script,
                    "max_pages_arg": None,
                    "split_preset_arg": None,
                    "split_every_arg": None,
                    "split_ocr_parallel_arg": False,
                    "workers_arg": workers,
                    "resource_guard_timeout_seconds_arg": resource_guard_timeout_seconds,
                    "resource_guard_policy_arg": (
                        _RESOURCE_GUARD_POLICY_FAIL_OPEN
                        if resource_guard_fail_open
                        else _RESOURCE_GUARD_POLICY_FAIL_CLOSED
                    ),
                    "resolved_ocr_mode": ocr_mode,
                    "progress_format_arg": progress_format,
                }
                return _execute_conversion(**execute_kwargs)

            def _write_chunk_progress(*, chunk_index: int, start_page: int, end_page: int) -> None:
                _write_progress(
                    95,
                    (
                        "writing markdown output "
                        f"chunk_index={chunk_index} chunk_total={chunk_total} "
                        f"page_range={start_page}-{end_page}"
                    ),
                )

            def _run_serial_chunk_loop() -> int:
                for chunk_index, start_page, end_page, split_pdf in chunk_jobs:
                    chunk_output = _build_chunk_output_path(output_base, start_page, end_page)
                    exit_code = _run_single_chunk(split_pdf=split_pdf, chunk_output=chunk_output)
                    if exit_code != 0:
                        return exit_code
                    _write_chunk_progress(
                        chunk_index=chunk_index,
                        start_page=start_page,
                        end_page=end_page,
                    )
                return 0

            if not split_ocr_parallel or len(chunk_jobs) <= 1:
                serial_exit_code = _run_serial_chunk_loop()
                if serial_exit_code != 0:
                    return serial_exit_code
            else:
                parallel_workers = _compute_parallel_workers(len(chunk_jobs))
                if parallel_workers <= 1:
                    _write_stderr_line(
                        "Split+OCR parallel mode requested but worker budget is 1; falling back to serial execution."
                    )
                    serial_exit_code = _run_serial_chunk_loop()
                    if serial_exit_code != 0:
                        return serial_exit_code
                else:
                    try:
                        _wait_for_resource_headroom(
                            timeout_seconds=resource_guard_timeout_seconds,
                            fail_open=resource_guard_fail_open,
                        )
                    except RuntimeError as error:
                        if not resource_guard_fail_open:
                            _write_stderr_line(str(error))
                            return 1
                        _write_stderr_line(
                            f"{error} falling back to serial split+ocr execution"
                        )
                        serial_exit_code = _run_serial_chunk_loop()
                        if serial_exit_code != 0:
                            return serial_exit_code
                    else:
                        executor = ThreadPoolExecutor(max_workers=parallel_workers)
                        try:
                            future_map = {
                                executor.submit(
                                    _run_single_chunk,
                                    split_pdf=split_pdf,
                                    chunk_output=_build_chunk_output_path(
                                        output_base,
                                        start_page,
                                        end_page,
                                    ),
                                ): (chunk_index, start_page, end_page)
                                for chunk_index, start_page, end_page, split_pdf in chunk_jobs
                            }

                            for future in as_completed(future_map):
                                chunk_index, start_page, end_page = future_map[future]
                                exit_code = future.result()
                                if exit_code != 0:
                                    executor.shutdown(wait=False, cancel_futures=True)
                                    return exit_code
                                _write_chunk_progress(
                                    chunk_index=chunk_index,
                                    start_page=start_page,
                                    end_page=end_page,
                                )
                        finally:
                            executor.shutdown(wait=True, cancel_futures=False)
    finally:
        _OCR_EXTRACTOR_CACHE.clear()
        _ocr_extractor_cache_enabled = previous_cache_enabled

    _write_progress(100, f"done output={output_base} chunk_total={chunk_total}")
    return 0


def _resolve_chunk_output_base(output_path: Path, input_pdf: Path) -> Path:
    if output_path.exists() and output_path.is_dir():
        return output_path / input_pdf.stem
    if output_path.suffix.lower() == ".md":
        return output_path.with_suffix("")
    return output_path


def _build_chunk_output_path(base_path: Path, start_page: int, end_page: int) -> Path:
    return base_path.with_name(
        f"{base_path.name}_p{start_page:04d}-{end_page:04d}.md"
    )


def _compute_parallel_workers(total_tasks: int) -> int:
    if total_tasks <= 0:
        return 1
    cpu_count = os.cpu_count() or 1
    target = max(1, int(cpu_count * 0.7))
    workers = min(total_tasks, target)

    cpu_ratio = _current_cpu_usage_ratio()
    if cpu_ratio is not None:
        cpu_headroom = max(0.0, _RESOURCE_USAGE_LIMIT - cpu_ratio)
        cpu_budget_workers = max(1, int(cpu_count * cpu_headroom))
        workers = min(workers, cpu_budget_workers)

    mem_budget_workers = _memory_budget_worker_cap(total_tasks)
    if mem_budget_workers is not None:
        workers = min(workers, mem_budget_workers)

    return max(1, min(total_tasks, workers))


def _resolve_parallel_workers(total_tasks: int, requested_workers: int | None = None) -> int:
    resolved_workers = _compute_parallel_workers(total_tasks)
    if requested_workers is None:
        return resolved_workers
    return max(1, min(total_tasks, resolved_workers, requested_workers))


def _read_meminfo_bytes() -> tuple[int, int] | None:
    try:
        meminfo = Path("/proc/meminfo").read_text(encoding="utf-8")
    except Exception:
        return None

    total_kb: int | None = None
    available_kb: int | None = None
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                total_kb = int(parts[1])
        elif line.startswith("MemAvailable:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                available_kb = int(parts[1])

    if total_kb is None or available_kb is None or total_kb <= 0:
        return None
    return total_kb * 1024, available_kb * 1024


def _current_process_rss_bytes() -> int:
    try:
        status_text = Path("/proc/self/status").read_text(encoding="utf-8")
    except Exception:
        return 0
    for line in status_text.splitlines():
        if not line.startswith("VmRSS:"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            return int(parts[1]) * 1024
    return 0


def _current_memory_usage_ratio() -> float | None:
    memory_info = _read_meminfo_bytes()
    if memory_info is None:
        return None
    total_bytes, available_bytes = memory_info
    used_ratio = 1.0 - (available_bytes / total_bytes)
    return max(0.0, min(1.0, used_ratio))


def _current_cpu_usage_ratio() -> float | None:
    cpu_count = os.cpu_count() or 1
    try:
        load_avg = os.getloadavg()[0]
    except Exception:
        return None
    return max(0.0, load_avg / max(1, cpu_count))


def _memory_budget_worker_cap(total_tasks: int) -> int | None:
    memory_info = _read_meminfo_bytes()
    if memory_info is None:
        return None
    total_bytes, _available_bytes = memory_info
    current_ratio = _current_memory_usage_ratio()
    if current_ratio is None:
        return None
    if current_ratio >= _RESOURCE_USAGE_LIMIT:
        return 1

    budget_bytes = int(total_bytes * (_RESOURCE_USAGE_LIMIT - current_ratio))
    process_rss = _current_process_rss_bytes()
    estimated_per_worker = max(64 * 1024 * 1024, process_rss // max(1, os.cpu_count() or 1))
    if estimated_per_worker <= 0:
        return None
    return max(1, min(total_tasks, budget_bytes // estimated_per_worker))


def _resource_usage_exceeded() -> bool:
    cpu_ratio = _current_cpu_usage_ratio()
    if cpu_ratio is not None and cpu_ratio >= _RESOURCE_USAGE_LIMIT:
        return True
    memory_ratio = _current_memory_usage_ratio()
    if memory_ratio is not None and memory_ratio >= _RESOURCE_USAGE_LIMIT:
        return True
    return False


def _format_ratio_for_progress(ratio: float | None) -> str:
    if ratio is None:
        return "n/a"
    return f"{ratio * 100:.1f}%"


def _wait_for_resource_headroom(
    *,
    timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
) -> None:
    elapsed_seconds = 0.0
    last_heartbeat_seconds = -_RESOURCE_WAIT_HEARTBEAT_SECONDS
    while _resource_usage_exceeded():
        if elapsed_seconds >= timeout_seconds:
            cpu_ratio = _current_cpu_usage_ratio()
            memory_ratio = _current_memory_usage_ratio()
            timeout_message = (
                "Resource guard: max wait reached; "
                f"elapsed={int(elapsed_seconds)}s "
                f"cpu={_format_ratio_for_progress(cpu_ratio)} "
                f"memory={_format_ratio_for_progress(memory_ratio)} "
                f"limit={int(_RESOURCE_USAGE_LIMIT * 100)}%"
            )
            if fail_open:
                _write_stderr_line(f"{timeout_message} continuing with fail-open policy")
                break
            raise RuntimeError(timeout_message)
        if elapsed_seconds - last_heartbeat_seconds >= _RESOURCE_WAIT_HEARTBEAT_SECONDS:
            cpu_ratio = _current_cpu_usage_ratio()
            memory_ratio = _current_memory_usage_ratio()
            _write_stderr_line(
                "Resource guard: waiting for headroom "
                f"elapsed={int(elapsed_seconds)}s "
                f"cpu={_format_ratio_for_progress(cpu_ratio)} "
                f"memory={_format_ratio_for_progress(memory_ratio)} "
                f"limit={int(_RESOURCE_USAGE_LIMIT * 100)}%"
            )
            last_heartbeat_seconds = elapsed_seconds
        time.sleep(_RESOURCE_POLL_SECONDS)
        elapsed_seconds += _RESOURCE_POLL_SECONDS


def _run_ocr_fallback_pipeline(
    *,
    input_pdf: Path,
    page_texts: list[str],
    requested_engine: str,
    layout_mode: str,
    strict_mode: bool,
    key_content_fallback_enabled: bool,
    workers: int | None = None,
    resource_guard_timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    resource_guard_fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
) -> OcrFallbackPipelineResult:
    quality_thresholds = _resolve_ocr_quality_threshold_profile(
        backend=requested_engine,
        layout_mode=layout_mode,
    )
    key_content_min_page_score = _resolve_key_content_min_page_score(
        layout_mode=layout_mode,
    )

    weak_pages_before_pdfplumber = sum(1 for page_text in page_texts if _is_weak_page_text(page_text))
    _write_progress(
        45,
        (
            "running layout assist "
            f"weak_pages_before_pdfplumber={weak_pages_before_pdfplumber}"
        ),
    )
    merged_page_texts = _apply_selective_pdfplumber_route(
        input_pdf,
        page_texts,
        diagnostics_writer=_write_stderr_line,
    )

    weak_page_indices = [
        index for index, page_text in enumerate(merged_page_texts) if _is_weak_page_text(page_text)
    ]
    weak_pages_after_pdfplumber = len(weak_page_indices)
    ocr_pages_requested = len(weak_page_indices)
    ocr_pages_applied = 0
    ocr_retry_count = 0
    resolved_ocr_engine = requested_engine

    if weak_page_indices:
        _write_progress(
            60,
            (
                "running ocr "
                f"backend={requested_engine} pages={ocr_pages_requested}"
            ),
        )
        ocr_page_progress_writer = _build_page_progress_writer(
            stage_label="ocr page progress",
            range_start=60,
            range_end=82,
        )
        ocr_page_texts: list[str] = []
        should_abort = False

        def _emit_failure_diagnostics(
            *,
            dependency_missing_module: str | None = None,
            backend_error: str | None = None,
        ) -> None:
            _write_stderr_line(
                _ocr_diagnostics_message(
                    requested_engine=requested_engine,
                    weak_pages_before_pdfplumber=weak_pages_before_pdfplumber,
                    weak_pages_after_pdfplumber=weak_pages_after_pdfplumber,
                    ocr_pages_requested=ocr_pages_requested,
                    ocr_pages_applied=ocr_pages_applied,
                    ocr_retry_count=ocr_retry_count,
                    resolved_engine=resolved_ocr_engine,
                    dependency_missing_module=dependency_missing_module,
                    backend_error=backend_error,
                )
            )

        try:
            ocr_page_texts, ocr_retry_count = _run_ocr_backend_with_transient_retry(
                input_pdf=input_pdf,
                requested_engine=requested_engine,
                page_count=len(merged_page_texts),
                layout_mode=layout_mode,
                page_indices=weak_page_indices,
                progress_callback=ocr_page_progress_writer,
                workers=workers,
                resource_guard_timeout_seconds=resource_guard_timeout_seconds,
                resource_guard_fail_open=resource_guard_fail_open,
            )
        except ModuleNotFoundError as error:
            missing_dependency_message = _missing_ocr_dependency_message(requested_engine, error.name)
            if strict_mode:
                _write_stderr_line(missing_dependency_message)
            else:
                _write_stderr_line(_auto_ocr_skip_message(missing_dependency_message))
            _emit_failure_diagnostics(dependency_missing_module=error.name)
            should_abort = strict_mode
        except (RuntimeError, OSError, TimeoutError) as error:
            ocr_retry_count = int(getattr(error, "_ocr_retry_count", ocr_retry_count))
            runtime_error_message = str(error)
            if strict_mode:
                _write_stderr_line(runtime_error_message)
            else:
                _write_stderr_line(_auto_ocr_skip_message(runtime_error_message))
            _emit_failure_diagnostics(backend_error=error.__class__.__name__)
            should_abort = strict_mode

        if should_abort:
            return OcrFallbackPipelineResult(
                page_texts=merged_page_texts,
                weak_pages_before_pdfplumber=weak_pages_before_pdfplumber,
                weak_pages_after_pdfplumber=weak_pages_after_pdfplumber,
                ocr_pages_requested=ocr_pages_requested,
                ocr_pages_applied=ocr_pages_applied,
                resolved_ocr_engine=resolved_ocr_engine,
                should_abort=True,
            )

        for list_offset, page_index in enumerate(weak_page_indices):
            if list_offset >= len(ocr_page_texts):
                break
            baseline_page_text = merged_page_texts[page_index]
            normalized_ocr_page_text = _normalize_page_text(ocr_page_texts[list_offset])
            if _should_replace_page_with_ocr(
                baseline_text=baseline_page_text,
                ocr_text=normalized_ocr_page_text,
                quality_thresholds=quality_thresholds,
            ):
                merged_page_texts[page_index] = normalized_ocr_page_text
                ocr_pages_applied += 1
            elif key_content_fallback_enabled and _should_use_key_content_fallback(
                baseline_text=baseline_page_text,
                ocr_text=normalized_ocr_page_text,
                min_page_score=key_content_min_page_score,
            ):
                merged_page_texts[page_index] = _render_key_content_fallback_page(
                    page_number=page_index + 1,
                    source_text=normalized_ocr_page_text,
                )
                ocr_pages_applied += 1

        _write_progress(
            82,
            f"ocr stage complete applied_pages={ocr_pages_applied}",
        )

    _write_stderr_line(
        _ocr_diagnostics_message(
            requested_engine=requested_engine,
            weak_pages_before_pdfplumber=weak_pages_before_pdfplumber,
            weak_pages_after_pdfplumber=weak_pages_after_pdfplumber,
            ocr_pages_requested=ocr_pages_requested,
            ocr_pages_applied=ocr_pages_applied,
            ocr_retry_count=ocr_retry_count,
            resolved_engine=resolved_ocr_engine,
        )
    )

    return OcrFallbackPipelineResult(
        page_texts=merged_page_texts,
        weak_pages_before_pdfplumber=weak_pages_before_pdfplumber,
        weak_pages_after_pdfplumber=weak_pages_after_pdfplumber,
        ocr_pages_requested=ocr_pages_requested,
        ocr_pages_applied=ocr_pages_applied,
        resolved_ocr_engine=resolved_ocr_engine,
        should_abort=False,
    )


def _render_chunk_markdown_text(
    chunk_page_texts: list[str],
    *,
    page_start: int,
    resource_guard_timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    resource_guard_fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
) -> str:
    _wait_for_resource_headroom(
        timeout_seconds=resource_guard_timeout_seconds,
        fail_open=resource_guard_fail_open,
    )
    return "".join(
        format_markdown_pages_streaming(
            chunk_page_texts,
            page_start=page_start,
        )
    )


def _write_chunk_markdown_file(
    *,
    chunk_output: Path,
    chunk_page_texts: list[str],
    page_start: int,
    resource_guard_timeout_seconds: float = _RESOURCE_WAIT_MAX_SECONDS,
    resource_guard_fail_open: bool = _RESOURCE_WAIT_FAIL_OPEN,
) -> str:
    markdown_text = _render_chunk_markdown_text(
        chunk_page_texts,
        page_start=page_start,
        resource_guard_timeout_seconds=resource_guard_timeout_seconds,
        resource_guard_fail_open=resource_guard_fail_open,
    )
    chunk_output.parent.mkdir(parents=True, exist_ok=True)
    write_markdown_stream(chunk_output, [markdown_text])
    return markdown_text


def convert_pdf_to_markdown(input_pdf: Path) -> str:
    page_texts = extract_page_texts(input_pdf)
    return format_markdown_pages(page_texts)


def _has_extractable_text(page_texts: list[str]) -> bool:
    return any(page_text.strip() for page_text in page_texts)


def _calculate_printable_ratio(text: str) -> float:
    if not text:
        return 0.0
    printable_count = sum(1 for character in text if character.isprintable())
    return printable_count / len(text)


def _is_weak_page_text(page_text: str) -> bool:
    stripped_text = page_text.strip()
    if not stripped_text:
        return True
    if len(stripped_text) < PDFPLUMBER_TRIGGER_MIN_CHAR_COUNT:
        return True
    printable_ratio = _calculate_printable_ratio(stripped_text)
    return printable_ratio < PDFPLUMBER_TRIGGER_MIN_PRINTABLE_RATIO


def _apply_selective_pdfplumber_route(
    input_pdf: Path,
    page_texts: list[str],
    diagnostics_writer: Callable[[str], None] | None = None,
) -> list[str]:
    weak_page_indices = [
        index for index, page_text in enumerate(page_texts) if _is_weak_page_text(page_text)
    ]
    selected_indices = weak_page_indices[:PDFPLUMBER_MAX_PAGES_PER_DOCUMENT]
    if not selected_indices:
        return page_texts

    try:
        fallback_text_by_index = _extract_page_raw_texts_with_pdfplumber(
            input_pdf, selected_indices
        )
    except Exception as error:
        if diagnostics_writer is not None:
            diagnostics_writer(_pdfplumber_fallback_warning(error))
        return page_texts

    merged_page_texts = list(page_texts)
    for page_index in selected_indices:
        fallback_text = _normalize_page_text(fallback_text_by_index.get(page_index, ""))
        if fallback_text and not _is_weak_page_text(fallback_text):
            merged_page_texts[page_index] = fallback_text

    return merged_page_texts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract text from a PDF and write markdown output."
    )
    _ = parser.add_argument("input_pdf", nargs="?", help="Path to the input PDF file")
    _ = parser.add_argument(
        "-o",
        "--output",
        help=(
            "Path to output markdown file "
            "(default: <project_root>/downloads/<input_stem>/<input_stem>.md)"
        ),
    )
    split_group = parser.add_mutually_exclusive_group()
    _ = split_group.add_argument(
        "--split-preset",
        type=int,
        choices=SPLIT_PRESET_CHOICES,
        help="Split output into preset page chunks (10, 20, 50, 100 pages).",
    )
    _ = split_group.add_argument(
        "--split-every",
        type=_positive_int,
        metavar="N",
        help="Split output into one markdown file every N pages.",
    )
    _ = parser.add_argument(
        "--split-ocr-parallel",
        action="store_true",
        help="Opt-in: run split+OCR chunk conversion in parallel (default: serial).",
    )
    _ = parser.add_argument(
        "--max-pages",
        type=_positive_int,
        metavar="N",
        help="Process only the first N pages (useful for fast test runs).",
    )
    _ = parser.add_argument(
        "--workers",
        type=_positive_int,
        metavar="N",
        help="Max worker count for internal parallel stages.",
    )
    _ = parser.add_argument(
        "--resource-guard-policy",
        default=_RESOURCE_GUARD_POLICY_FAIL_OPEN,
        choices=_RESOURCE_GUARD_POLICY_CHOICES,
        help="Resource guard timeout behavior: fail-open continues, fail-closed aborts.",
    )
    _ = parser.add_argument(
        "--resource-guard-timeout-seconds",
        type=_positive_float,
        default=_RESOURCE_WAIT_MAX_SECONDS,
        metavar="SECONDS",
        help="Max seconds to wait for resource headroom before applying guard policy.",
    )
    _ = parser.add_argument(
        "--progress-format",
        default=PROGRESS_FORMAT_TEXT,
        choices=PROGRESS_FORMAT_CHOICES,
        help="Progress output format: text (default) or jsonl.",
    )
    _ = parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output file if it already exists",
    )
    _ = parser.add_argument(
        "--wizard",
        action="store_true",
        help="Run an interactive terminal wizard to choose options",
    )
    _ = parser.add_argument(
        "--ctl",
        action="store_true",
        help="Run full interactive terminal mode for selecting all options",
    )
    _ = parser.add_argument(
        "--ocr-fallback",
        action="store_true",
        help="Use OCR when text layer extraction returns empty pages",
    )
    _ = parser.add_argument(
        "--ocr",
        choices=(OCR_MODE_AUTO,),
        help="Compatibility flag. `auto` enables non-fatal OCR fallback attempts.",
    )
    _ = parser.add_argument(
        "--ocr-engine",
        default=OCR_DEFAULT_ENGINE,
        choices=("rapidocr",),
        help="Select OCR backend when --ocr-fallback is active.",
    )
    _ = parser.add_argument(
        "--ocr-layout",
        default=OCR_LAYOUT_AUTO,
        choices=(OCR_LAYOUT_AUTO, OCR_LAYOUT_VERTICAL, OCR_LAYOUT_HORIZONTAL),
        help="Hint OCR reading order: auto, vertical (right-to-left columns), or horizontal.",
    )
    _ = parser.add_argument(
        "--ocr-classical-zh-postprocess",
        action="store_true",
        help="Apply conservative phrase-level corrections for historical/classical Chinese OCR output.",
    )
    _ = parser.add_argument(
        "--ocr-key-content-fallback",
        action="store_true",
        help="For low-confidence pages, emit key-content lines with fallback marker instead of full OCR text.",
    )
    _ = parser.add_argument(
        "--zh-script",
        default=ZH_SCRIPT_KEEP,
        choices=ZH_SCRIPT_CHOICES,
        help=(
            "Normalize Chinese output script: keep, hant (Traditional), or hans "
            "(Simplified)."
        ),
    )
    return parser


def _apply_zh_script_conversion(
    page_texts: list[str],
    *,
    target_script: str,
) -> tuple[list[str], str]:
    if target_script == ZH_SCRIPT_KEEP:
        diagnostics = "zh_script_conversion: target=keep changed_pages=0"
        return page_texts, diagnostics

    if target_script not in _OPENCC_CONFIG_BY_ZH_SCRIPT:
        raise RuntimeError(f"Unsupported zh script target: {target_script}")

    try:
        opencc_module = importlib.import_module("opencc")
    except ModuleNotFoundError as error:
        raise RuntimeError(_zh_script_dependency_missing_message(target_script)) from error

    opencc_cls = getattr(opencc_module, "OpenCC", None)
    if not callable(opencc_cls):
        raise RuntimeError("OpenCC module is missing OpenCC constructor")

    converter = cast(object, opencc_cls(_OPENCC_CONFIG_BY_ZH_SCRIPT[target_script]))
    convert_callable = getattr(converter, "convert", None)
    if not callable(convert_callable):
        raise RuntimeError("OpenCC converter is missing convert()")

    converted_pages: list[str] = []
    changed_pages = 0
    for page_text in page_texts:
        converted_text_raw = cast(object, convert_callable(page_text))
        converted_text = converted_text_raw if isinstance(converted_text_raw, str) else str(converted_text_raw)
        converted_pages.append(converted_text)
        if converted_text != page_text:
            changed_pages += 1

    diagnostics = (
        "zh_script_conversion: "
        f"target={target_script} changed_pages={changed_pages} total_pages={len(page_texts)}"
    )
    return converted_pages, diagnostics


def _is_interactive_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _prompt_text(prompt: str, default: str | None = None) -> str:
    raw_value = input(prompt).strip()
    if raw_value:
        return raw_value
    return default or ""


def _prompt_choice(prompt: str, choices: tuple[str, ...], default: str) -> str:
    allowed_choices = {choice.lower(): choice for choice in choices}
    while True:
        selected = input(prompt).strip().lower()
        if not selected:
            return default
        if selected in allowed_choices:
            return allowed_choices[selected]
        _ = sys.stderr.write(
            f"Invalid selection '{selected}'. Choose one of: {', '.join(choices)}.\n"
        )


def _prompt_yes_no(prompt: str, *, default: bool) -> bool:
    while True:
        selected = input(prompt).strip().lower()
        if not selected:
            return default
        if selected in ("y", "yes"):
            return True
        if selected in ("n", "no"):
            return False
        _ = sys.stderr.write("Invalid selection. Enter yes/y or no/n.\n")


def _resolve_wizard_options(
    input_pdf: str | None,
    output_path: str | None,
    force: bool,
) -> tuple[str, str | None, bool, bool, str]:
    profile_choices = tuple(WIZARD_PRESET_PROFILES)
    profile = _prompt_choice(
        "Select profile [fast/balanced/accurate] (default: balanced): ",
        profile_choices,
        "balanced",
    )
    preset = WIZARD_PRESET_PROFILES[profile]
    resolved_ocr_fallback = preset.ocr_fallback
    resolved_ocr_engine = OCR_DEFAULT_ENGINE

    if resolved_ocr_fallback:
        _write_stderr_line("OCR backend is fixed to rapidocr.")
    else:
        resolved_ocr_fallback = _prompt_yes_no(
            "Enable OCR fallback anyway? [y/N]: ",
            default=False,
        )
        if resolved_ocr_fallback:
            _write_stderr_line("OCR backend is fixed to rapidocr.")
            resolved_ocr_engine = OCR_DEFAULT_ENGINE

    resolved_input_pdf = _prompt_text(
        (
            f"Input PDF path [{input_pdf}]: "
            if input_pdf
            else "Input PDF path: "
        ),
        input_pdf,
    )

    default_output = output_path
    if default_output is None and resolved_input_pdf:
        default_output = str(_resolve_default_output_path(Path(resolved_input_pdf)))

    resolved_output = _prompt_text(
        (
            f"Output markdown path [{default_output}]: "
            if default_output
            else (
                "Output markdown path "
                "(blank for <project_root>/downloads/<input_stem>/<input_stem>.md): "
            )
        ),
        default_output,
    )
    resolved_force = force or _prompt_yes_no(
        "Overwrite output if it already exists? [y/N]: ",
        default=False,
    )

    return (
        resolved_input_pdf,
        resolved_output or None,
        resolved_force,
        resolved_ocr_fallback,
        resolved_ocr_engine,
    )


def _resolve_ctl_options(
    input_pdf: str | None,
    output_path: str | None,
    force: bool,
) -> tuple[str, str | None, bool, str, str, str, bool]:
    resolved_input_pdf = _prompt_text(
        (
            f"Input PDF path [{input_pdf}]: "
            if input_pdf
            else "Input PDF path: "
        ),
        input_pdf,
    )

    default_output = output_path
    if default_output is None and resolved_input_pdf:
        default_output = str(_resolve_default_output_path(Path(resolved_input_pdf)))
    resolved_output = _prompt_text(
        (
            f"Output markdown path [{default_output}]: "
            if default_output
            else (
                "Output markdown path "
                "(blank for <project_root>/downloads/<input_stem>/<input_stem>.md): "
            )
        ),
        default_output,
    )

    resolved_force = force or _prompt_yes_no(
        "Overwrite output if it already exists? [y/N]: ",
        default=False,
    )

    ocr_mode = _prompt_choice(
        "OCR mode [off/strict/auto] (default: strict): ",
        ("off", "strict", "auto"),
        "strict",
    )

    resolved_engine = OCR_DEFAULT_ENGINE
    resolved_layout = OCR_LAYOUT_AUTO
    if ocr_mode in ("strict", "auto"):
        _write_stderr_line("OCR backend is fixed to rapidocr.")
        resolved_layout = _prompt_choice(
            "OCR layout [auto/vertical/horizontal] (default: auto): ",
            (OCR_LAYOUT_AUTO, OCR_LAYOUT_VERTICAL, OCR_LAYOUT_HORIZONTAL),
            OCR_LAYOUT_AUTO,
        )

    resolved_classical_postprocess = _prompt_yes_no(
        "Enable classical Chinese postprocess? [Y/n]: ",
        default=True,
    )

    return (
        resolved_input_pdf,
        resolved_output or None,
        resolved_force,
        ocr_mode,
        resolved_engine,
        resolved_layout,
        resolved_classical_postprocess,
    )


def _resolve_effective_resource_guard_fail_open(
    *,
    configured_fail_open: bool,
    estimated_page_count: int,
    ocr_enabled: bool,
) -> bool:
    if not configured_fail_open:
        return False
    if not ocr_enabled:
        return True
    return estimated_page_count < _RESOURCE_GUARD_AUTO_FAIL_CLOSED_PAGE_THRESHOLD


def _write_performance_summary_report(
    *,
    flow: str,
    output_target: str,
    estimated_page_count: int,
    stage_seconds: dict[str, float],
) -> None:
    report_path = Path.cwd() / _PERF_REPORT_PATH_DEFAULT
    report_path.parent.mkdir(parents=True, exist_ok=True)
    total_seconds = max(0.0, stage_seconds.get("total", 0.0))
    page_per_second = (
        (estimated_page_count / total_seconds)
        if estimated_page_count > 0 and total_seconds > 0.0
        else 0.0
    )
    report_lines = [
        "# Performance Summary (Last Run)",
        "",
        f"- flow: {flow}",
        f"- output_target: {output_target}",
        f"- estimated_page_count: {estimated_page_count}",
        f"- native_seconds: {stage_seconds.get('native', 0.0):.3f}",
        f"- ocr_seconds: {stage_seconds.get('ocr', 0.0):.3f}",
        f"- postprocess_seconds: {stage_seconds.get('postprocess', 0.0):.3f}",
        f"- write_seconds: {stage_seconds.get('write', 0.0):.3f}",
        f"- total_seconds: {total_seconds:.3f}",
        f"- pages_per_second: {page_per_second:.3f}",
    ]
    _ = report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def _execute_conversion(
    *,
    input_pdf_arg: str,
    output_arg: str | None,
    force_arg: bool,
    ocr_fallback_arg: bool,
    ocr_mode_arg: str | None,
    ocr_engine_arg: str,
    ocr_layout_arg: str,
    ocr_classical_zh_postprocess_arg: bool,
    ocr_key_content_fallback_arg: bool,
    zh_script_arg: str,
    max_pages_arg: int | None,
    split_preset_arg: int | None,
    split_every_arg: int | None,
    split_ocr_parallel_arg: bool,
    workers_arg: int | None,
    resource_guard_timeout_seconds_arg: float,
    resource_guard_policy_arg: str,
    resolved_ocr_mode: str,
    progress_format_arg: str = PROGRESS_FORMAT_TEXT,
) -> int:
    global _active_progress_format

    ocr_auto_mode_enabled = ocr_mode_arg == OCR_MODE_AUTO
    ocr_fallback_enabled = ocr_fallback_arg or ocr_auto_mode_enabled
    ocr_strict_mode = ocr_fallback_arg

    if progress_format_arg not in PROGRESS_FORMAT_CHOICES:
        raise RuntimeError(
            f"Invalid progress format `{progress_format_arg}`. "
            f"Choose one of: {', '.join(PROGRESS_FORMAT_CHOICES)}"
        )

    input_pdf = Path(input_pdf_arg)
    output_path = Path(output_arg) if output_arg else _resolve_default_output_path(input_pdf)
    chunk_size = split_every_arg if split_every_arg is not None else split_preset_arg

    if not input_pdf.exists() or not input_pdf.is_file():
        _write_stderr_line(f"Input PDF not found: {input_pdf}")
        return 1

    estimated_page_count = _extract_page_count(input_pdf)
    if max_pages_arg is not None:
        estimated_page_count = min(estimated_page_count, max(1, max_pages_arg))
    configured_fail_open = resource_guard_policy_arg == _RESOURCE_GUARD_POLICY_FAIL_OPEN
    resource_guard_fail_open = _resolve_effective_resource_guard_fail_open(
        configured_fail_open=configured_fail_open,
        estimated_page_count=estimated_page_count,
        ocr_enabled=ocr_fallback_enabled,
    )

    if chunk_size is None and output_path.exists() and not force_arg:
        _write_stderr_line(
            f"Output file already exists: {output_path}. Use --force to overwrite."
        )
        return 1

    if output_arg is None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    previous_progress_format = _active_progress_format
    _active_progress_format = progress_format_arg
    conversion_started_at = time.monotonic()
    stage_seconds: dict[str, float] = {
        "native": 0.0,
        "ocr": 0.0,
        "postprocess": 0.0,
        "write": 0.0,
    }

    def _format_stage_seconds(value: float) -> str:
        return f"{max(0.0, value):.3f}"

    try:
        native_page_progress_writer = _build_page_progress_writer(
            stage_label="native page progress",
            range_start=15,
            range_end=35,
        )
        _write_progress(0, "starting conversion")
        _write_stderr_line(
            "Diagnostics: mode=resource_guard "
            f"configured_policy={resource_guard_policy_arg} "
            f"resolved_policy={'fail-open' if resource_guard_fail_open else 'fail-closed'} "
            f"estimated_pages={estimated_page_count}"
        )
        _write_progress(15, "extracting native text")
        native_started_at = time.monotonic()
        if chunk_size is not None and ocr_fallback_enabled:
            total_pages = estimated_page_count
            stage_seconds["native"] = time.monotonic() - native_started_at
            _write_progress(35, f"native extraction complete pages={total_pages}")
            write_started_at = time.monotonic()
            split_exit_code = _run_split_before_ocr_conversion(
                input_pdf=input_pdf,
                output_path=output_path,
                total_pages=total_pages,
                chunk_size=chunk_size,
                force=force_arg,
                ocr_mode=resolved_ocr_mode,
                ocr_engine=ocr_engine_arg,
                ocr_layout=ocr_layout_arg,
                ocr_classical_zh_postprocess=ocr_classical_zh_postprocess_arg,
                ocr_key_content_fallback=ocr_key_content_fallback_arg,
                zh_script=zh_script_arg,
                split_ocr_parallel=split_ocr_parallel_arg,
                workers=workers_arg,
                resource_guard_timeout_seconds=resource_guard_timeout_seconds_arg,
                resource_guard_fail_open=resource_guard_fail_open,
                progress_format=progress_format_arg,
            )
            stage_seconds["write"] = time.monotonic() - write_started_at
            total_seconds = time.monotonic() - conversion_started_at
            stage_seconds["total"] = total_seconds
            _write_stderr_line(
                "Diagnostics: mode=stage_timing "
                f"native_seconds={_format_stage_seconds(stage_seconds['native'])} "
                "ocr_seconds=0.000 "
                "postprocess_seconds=0.000 "
                f"write_seconds={_format_stage_seconds(stage_seconds['write'])} "
                f"total_seconds={_format_stage_seconds(total_seconds)} "
                "flow=split_before_ocr"
            )
            try:
                _write_performance_summary_report(
                    flow="split_before_ocr",
                    output_target=str(output_path),
                    estimated_page_count=total_pages,
                    stage_seconds=stage_seconds,
                )
            except Exception as report_error:
                _write_stderr_line(
                    f"Diagnostics: mode=perf_report status=failed error={report_error.__class__.__name__}"
                )
            return split_exit_code

        page_texts = extract_page_texts(
            input_pdf,
            max_pages=max_pages_arg,
            progress_callback=native_page_progress_writer,
        )
        stage_seconds["native"] = time.monotonic() - native_started_at
        _write_progress(35, f"native extraction complete pages={len(page_texts)}")

        if ocr_fallback_enabled:
            ocr_started_at = time.monotonic()
            pipeline_result = _run_ocr_fallback_pipeline(
                input_pdf=input_pdf,
                page_texts=page_texts,
                requested_engine=ocr_engine_arg,
                layout_mode=ocr_layout_arg,
                strict_mode=ocr_strict_mode,
                key_content_fallback_enabled=ocr_key_content_fallback_arg,
                workers=workers_arg,
                resource_guard_timeout_seconds=resource_guard_timeout_seconds_arg,
                resource_guard_fail_open=resource_guard_fail_open,
            )
            stage_seconds["ocr"] = time.monotonic() - ocr_started_at
            page_texts = pipeline_result.page_texts
            if pipeline_result.should_abort:
                return 1

        if not _has_extractable_text(page_texts):
            _write_stderr_line(_no_extractable_text_warning(ocr_fallback_enabled))
        postprocess_started_at = time.monotonic()
        if ocr_classical_zh_postprocess_arg:
            _write_progress(90, "applying classical chinese postprocess")
            postprocess_result = _apply_classical_zh_postprocess(page_texts)
            page_texts = postprocess_result[0]
            postprocess_diagnostics = postprocess_result[1]
            _write_stderr_line(postprocess_diagnostics)
        if zh_script_arg != ZH_SCRIPT_KEEP:
            _write_progress(92, f"normalizing chinese script target={zh_script_arg}")
            script_conversion_result = _apply_zh_script_conversion(
                page_texts,
                target_script=zh_script_arg,
            )
            page_texts = script_conversion_result[0]
            _write_stderr_line(script_conversion_result[1])
        stage_seconds["postprocess"] = time.monotonic() - postprocess_started_at

        page_texts = [_normalize_page_text(page_text) for page_text in page_texts]
        write_started_at = time.monotonic()
        if chunk_size is None:
            _write_progress(95, "writing markdown output")
            total_pages = max(1, len(page_texts))
            parallel_workers = _resolve_parallel_workers(
                total_pages, requested_workers=workers_arg
            )
            internal_chunk_size = max(1, (total_pages + parallel_workers - 1) // parallel_workers)
            internal_chunks = list(_iter_page_chunks(page_texts, internal_chunk_size))
            rendered_chunk_map: dict[int, str] = {}
            _wait_for_resource_headroom(
                timeout_seconds=resource_guard_timeout_seconds_arg,
                fail_open=resource_guard_fail_open,
            )
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                future_map = {
                    executor.submit(
                        _render_chunk_markdown_text,
                        chunk_page_texts,
                        page_start=start_page,
                        resource_guard_timeout_seconds=resource_guard_timeout_seconds_arg,
                        resource_guard_fail_open=resource_guard_fail_open,
                    ): chunk_index
                    for chunk_index, (start_page, _end_page, chunk_page_texts) in enumerate(
                        internal_chunks,
                        start=1,
                    )
                }
                for future in as_completed(future_map):
                    chunk_index = future_map[future]
                    rendered_chunk_map[chunk_index] = future.result()

            ordered_chunks = [
                rendered_chunk_map[index]
                for index in range(1, len(internal_chunks) + 1)
            ]
            write_markdown_stream(output_path, ordered_chunks)
            _write_progress(100, f"done output={output_path}")
        else:
            chunked_pages = list(_iter_page_chunks(page_texts, chunk_size))
            chunk_total = len(chunked_pages)
            output_base = _resolve_chunk_output_base(output_path, input_pdf)
            chunk_jobs: list[tuple[int, int, int, Path, list[str]]] = []

            for chunk_index, (start_page, end_page, chunk_page_texts) in enumerate(
                chunked_pages,
                start=1,
            ):
                chunk_output = _build_chunk_output_path(output_base, start_page, end_page)
                if chunk_output.exists() and not force_arg:
                    _write_stderr_line(
                        f"Output file already exists: {chunk_output}. Use --force to overwrite."
                    )
                    return 1
                chunk_jobs.append((chunk_index, start_page, end_page, chunk_output, chunk_page_texts))

            parallel_workers = _resolve_parallel_workers(
                len(chunk_jobs), requested_workers=workers_arg
            )
            _wait_for_resource_headroom(
                timeout_seconds=resource_guard_timeout_seconds_arg,
                fail_open=resource_guard_fail_open,
            )
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                future_map = {
                    executor.submit(
                        _write_chunk_markdown_file,
                        chunk_output=chunk_output,
                        chunk_page_texts=chunk_page_texts,
                        page_start=start_page,
                        resource_guard_timeout_seconds=resource_guard_timeout_seconds_arg,
                        resource_guard_fail_open=resource_guard_fail_open,
                    ): (chunk_index, start_page, end_page)
                    for chunk_index, start_page, end_page, chunk_output, chunk_page_texts in chunk_jobs
                }

                for future in as_completed(future_map):
                    chunk_index, start_page, end_page = future_map[future]
                    _ = future.result()
                    _write_progress(
                        95,
                        (
                            "writing markdown output "
                            f"chunk_index={chunk_index} chunk_total={chunk_total} "
                            f"page_range={start_page}-{end_page}"
                        ),
                    )

            _write_progress(
                100,
                f"done output={output_base} chunk_total={chunk_total}",
            )
        stage_seconds["write"] = time.monotonic() - write_started_at
        total_seconds = time.monotonic() - conversion_started_at
        stage_seconds["total"] = total_seconds
        _write_stderr_line(
            "Diagnostics: mode=stage_timing "
            f"native_seconds={_format_stage_seconds(stage_seconds['native'])} "
            f"ocr_seconds={_format_stage_seconds(stage_seconds['ocr'])} "
            f"postprocess_seconds={_format_stage_seconds(stage_seconds['postprocess'])} "
            f"write_seconds={_format_stage_seconds(stage_seconds['write'])} "
            f"total_seconds={_format_stage_seconds(total_seconds)} "
            "flow=single_pass"
        )
        try:
            _write_performance_summary_report(
                flow="single_pass",
                output_target=str(output_path),
                estimated_page_count=max(1, len(page_texts)),
                stage_seconds=stage_seconds,
            )
        except Exception as report_error:
            _write_stderr_line(
                f"Diagnostics: mode=perf_report status=failed error={report_error.__class__.__name__}"
            )
    except Exception as error:
        _write_stderr_line(_conversion_failed_message(error, input_pdf=input_pdf))
        return 1
    finally:
        _active_progress_format = previous_progress_format

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    input_pdf_arg = getattr(args, "input_pdf", None)
    output_arg = getattr(args, "output", None)
    force_arg = getattr(args, "force", False)
    wizard_arg = getattr(args, "wizard", False)
    ctl_arg = getattr(args, "ctl", False)
    ocr_fallback_arg = getattr(args, "ocr_fallback", False)
    ocr_mode_arg = getattr(args, "ocr", None)
    ocr_engine_arg = getattr(args, "ocr_engine", OCR_DEFAULT_ENGINE)
    ocr_layout_arg = getattr(args, "ocr_layout", OCR_LAYOUT_AUTO)
    ocr_classical_zh_postprocess_arg = getattr(args, "ocr_classical_zh_postprocess", False)
    ocr_key_content_fallback_arg = bool(getattr(args, "ocr_key_content_fallback", False))
    zh_script_arg = getattr(args, "zh_script", ZH_SCRIPT_KEEP)
    max_pages_arg = getattr(args, "max_pages", None)
    workers_arg = getattr(args, "workers", None)
    resource_guard_policy_arg = getattr(
        args,
        "resource_guard_policy",
        _RESOURCE_GUARD_POLICY_FAIL_OPEN,
    )
    resource_guard_timeout_seconds_arg = getattr(
        args,
        "resource_guard_timeout_seconds",
        _RESOURCE_WAIT_MAX_SECONDS,
    )
    split_preset_arg = getattr(args, "split_preset", None)
    split_every_arg = getattr(args, "split_every", None)
    split_ocr_parallel_arg = bool(getattr(args, "split_ocr_parallel", False))
    progress_format_arg = getattr(args, "progress_format", PROGRESS_FORMAT_TEXT)
    resolved_ocr_mode = (
        "strict"
        if ocr_fallback_arg
        else ("auto" if ocr_mode_arg == OCR_MODE_AUTO else "off")
    )

    if not isinstance(wizard_arg, bool):
        _write_stderr_line("Invalid wizard argument.")
        return 1
    if not isinstance(ctl_arg, bool):
        _write_stderr_line("Invalid ctl argument.")
        return 1
    if wizard_arg and ctl_arg:
        _write_stderr_line("Choose only one interactive mode: --wizard or --ctl.")
        return 1
    if not wizard_arg and not ctl_arg and input_pdf_arg is None:
        parser.error("the following arguments are required: input_pdf")
    if input_pdf_arg is not None and not isinstance(input_pdf_arg, str):
        _write_stderr_line("Invalid input argument.")
        return 1
    if output_arg is not None and not isinstance(output_arg, str):
        _write_stderr_line("Invalid output argument.")
        return 1
    if not isinstance(force_arg, bool):
        _write_stderr_line("Invalid force argument.")
        return 1
    if not isinstance(ocr_fallback_arg, bool):
        _write_stderr_line("Invalid OCR fallback argument.")
        return 1
    if ocr_mode_arg is not None and not isinstance(ocr_mode_arg, str):
        _write_stderr_line("Invalid OCR mode argument.")
        return 1
    if not isinstance(ocr_engine_arg, str):
        _write_stderr_line("Invalid OCR engine argument.")
        return 1
    if not isinstance(ocr_layout_arg, str):
        _write_stderr_line("Invalid OCR layout argument.")
        return 1
    if not isinstance(ocr_classical_zh_postprocess_arg, bool):
        _write_stderr_line("Invalid OCR classical zh postprocess argument.")
        return 1
    if not isinstance(zh_script_arg, str):
        _write_stderr_line("Invalid zh script argument.")
        return 1
    if zh_script_arg not in ZH_SCRIPT_CHOICES:
        _write_stderr_line("Invalid zh script argument.")
        return 1
    if max_pages_arg is not None and not isinstance(max_pages_arg, int):
        _write_stderr_line("Invalid max pages argument.")
        return 1
    if workers_arg is not None and not isinstance(workers_arg, int):
        _write_stderr_line("Invalid workers argument.")
        return 1
    if not isinstance(resource_guard_policy_arg, str):
        _write_stderr_line("Invalid resource guard policy argument.")
        return 1
    if resource_guard_policy_arg not in _RESOURCE_GUARD_POLICY_CHOICES:
        _write_stderr_line("Invalid resource guard policy argument.")
        return 1
    if not isinstance(resource_guard_timeout_seconds_arg, (int, float)):
        _write_stderr_line("Invalid resource guard timeout argument.")
        return 1
    resource_guard_timeout_seconds_arg = float(resource_guard_timeout_seconds_arg)
    if resource_guard_timeout_seconds_arg <= 0.0:
        _write_stderr_line("Invalid resource guard timeout argument.")
        return 1
    if split_preset_arg is not None and not isinstance(split_preset_arg, int):
        _write_stderr_line("Invalid split preset argument.")
        return 1
    if split_every_arg is not None and not isinstance(split_every_arg, int):
        _write_stderr_line("Invalid split every argument.")
        return 1
    if not isinstance(progress_format_arg, str):
        _write_stderr_line("Invalid progress format argument.")
        return 1
    if progress_format_arg not in PROGRESS_FORMAT_CHOICES:
        _write_stderr_line(
            f"Invalid progress format `{progress_format_arg}`. "
            f"Choose one of: {', '.join(PROGRESS_FORMAT_CHOICES)}"
        )
        return 1
    if wizard_arg:
        if not _is_interactive_tty():
            _write_stderr_line(
                "--wizard requires a TTY in non-interactive environments. Re-run without --wizard and pass flags explicitly."
            )
            return 1
        (
            input_pdf_arg,
            output_arg,
            force_arg,
            ocr_fallback_arg,
            ocr_engine_arg,
        ) = _resolve_wizard_options(
            input_pdf_arg,
            output_arg,
            force_arg,
        )

    if ctl_arg:
        if not _is_interactive_tty():
            _write_stderr_line(
                "--ctl requires a TTY in non-interactive environments. Re-run without --ctl and pass flags explicitly."
            )
            return 1
        (
            input_pdf_arg,
            output_arg,
            force_arg,
            resolved_ocr_mode,
            ocr_engine_arg,
            ocr_layout_arg,
            ocr_classical_zh_postprocess_arg,
        ) = _resolve_ctl_options(
            input_pdf_arg,
            output_arg,
            force_arg,
        )
        if resolved_ocr_mode == "off":
            ocr_fallback_arg = False
            ocr_mode_arg = None
            ocr_layout_arg = OCR_LAYOUT_AUTO
        elif resolved_ocr_mode == "strict":
            ocr_fallback_arg = True
            ocr_mode_arg = None
        else:
            ocr_fallback_arg = False
            ocr_mode_arg = OCR_MODE_AUTO

    if not isinstance(input_pdf_arg, str) or not input_pdf_arg.strip():
        _write_stderr_line("Invalid input argument.")
        return 1

    return _execute_conversion(
        input_pdf_arg=input_pdf_arg,
        output_arg=output_arg,
        force_arg=force_arg,
        ocr_fallback_arg=ocr_fallback_arg,
        ocr_mode_arg=ocr_mode_arg,
        ocr_engine_arg=ocr_engine_arg,
        ocr_layout_arg=ocr_layout_arg,
        ocr_classical_zh_postprocess_arg=ocr_classical_zh_postprocess_arg,
        ocr_key_content_fallback_arg=ocr_key_content_fallback_arg,
        zh_script_arg=zh_script_arg,
        max_pages_arg=max_pages_arg,
        split_preset_arg=split_preset_arg,
        split_every_arg=split_every_arg,
        split_ocr_parallel_arg=split_ocr_parallel_arg,
        workers_arg=workers_arg,
        resource_guard_timeout_seconds_arg=resource_guard_timeout_seconds_arg,
        resource_guard_policy_arg=resource_guard_policy_arg,
        resolved_ocr_mode=resolved_ocr_mode,
        progress_format_arg=progress_format_arg,
    )


if __name__ == "__main__":
    raise SystemExit(main())
