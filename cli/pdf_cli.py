from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path
from typing import cast

import pdf_to_md


_HYBRID_COMMANDS: frozenset[str] = frozenset({"convert", "init", "config", "profile"})
_LEGACY_COMPAT_WARNING_CLASS = "legacy-invocation"
_LEGACY_COMPAT_WARNING_MESSAGE = (
    "DeprecationWarning: legacy invocation is supported in v1; "
    "migrate to `pdftomd convert ...` before v2."
)
_CONFIG_ENV_PATH = "PDF_TO_MD_CONFIG"
_CONFIG_DEFAULT_NAME = ".pdf-to-md.json"
_RESOURCE_DIR_NAME = "resource"
_DEFAULT_DOWNLOADS_DIR_NAME = "downloads"
_DELIVERY_IMMEDIATE = "immediate"
_DELIVERY_BATCH = "batch"
_OCR_DEPENDENCY_ACTION_ABORT = "abort"
_OCR_DEPENDENCY_ACTION_AUTO = "auto"
_OCR_DEPENDENCY_ACTION_OFF = "off"
_PROGRESS_DETAIL_VERBOSE = "verbose"
_PROGRESS_DETAIL_COMPACT = "compact"
_PROGRESS_INTERVAL_DEFAULT_SECONDS = 1.0
_PROGRESS_INTERVAL_MIN_SECONDS = 0.2
_LIVE_MONITOR_MAX_SECONDS = 60.0 * 60.0
_LIVE_MONITOR_TERMINATE_GRACE_SECONDS = 5.0
_ENV_MAP: dict[str, str] = {
    "output": "PDF_TO_MD_OUTPUT",
    "force": "PDF_TO_MD_FORCE",
    "ocr_mode": "PDF_TO_MD_OCR_MODE",
    "ocr_engine": "PDF_TO_MD_OCR_ENGINE",
    "ocr_layout": "PDF_TO_MD_OCR_LAYOUT",
    "zh_script": "PDF_TO_MD_ZH_SCRIPT",
    "classical_zh_postprocess": "PDF_TO_MD_CLASSICAL_ZH_POSTPROCESS",
    "key_content_fallback": "PDF_TO_MD_KEY_CONTENT_FALLBACK",
    "split_preset": "PDF_TO_MD_SPLIT_PRESET",
    "split_every": "PDF_TO_MD_SPLIT_EVERY",
    "workers": "PDF_TO_MD_WORKERS",
    "profile": "PDF_TO_MD_PROFILE",
}
_OPTION_KEYS: tuple[str, ...] = (
    "output",
    "force",
    "ocr_mode",
    "ocr_engine",
    "ocr_layout",
    "zh_script",
    "classical_zh_postprocess",
    "key_content_fallback",
    "split_preset",
    "split_every",
    "workers",
)
_DEFAULTS: dict[str, object] = {
    "force": False,
    "ocr_mode": "off",
    "ocr_engine": "rapidocr",
    "ocr_layout": "auto",
    "zh_script": "keep",
    "classical_zh_postprocess": False,
    "key_content_fallback": False,
    "split_preset": None,
    "split_every": None,
    "workers": None,
}
_ROOT_ALLOWED_KEYS: frozenset[str] = frozenset({"active_profile", "profiles", *_OPTION_KEYS})
_PROFILE_ALLOWED_KEYS: frozenset[str] = frozenset(_OPTION_KEYS)
_PROGRESS_LINE_PATTERN = re.compile(
    r"^Progress:\s*(?P<percent>\d+)%\s*(?P<stage>.*)$",
    re.IGNORECASE,
)


class _LiveMonitorContext:
    input_pdf: Path
    output_arg: str
    split_selected: bool
    delivery_mode: str
    execution_mode_hint: str
    progress_detail: str
    progress_interval_seconds: float

    def __init__(
        self,
        *,
        input_pdf: Path,
        output_arg: str,
        split_selected: bool,
        delivery_mode: str,
        execution_mode_hint: str,
        progress_detail: str,
        progress_interval_seconds: float,
    ) -> None:
        self.input_pdf = input_pdf
        self.output_arg = output_arg
        self.split_selected = split_selected
        self.delivery_mode = delivery_mode
        self.execution_mode_hint = execution_mode_hint
        self.progress_detail = progress_detail
        self.progress_interval_seconds = max(
            _PROGRESS_INTERVAL_MIN_SECONDS,
            progress_interval_seconds,
        )


_live_monitor_context: _LiveMonitorContext | None = None


class CliRuntimeValidationError(ValueError):
    pass


def _coerce_bool(raw_value: object, context: str) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        lowered = raw_value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise CliRuntimeValidationError(f"Invalid boolean for {context}: {raw_value!r}")


def _coerce_mode(raw_value: object, context: str) -> str:
    if not isinstance(raw_value, str):
        raise CliRuntimeValidationError(f"Invalid ocr_mode for {context}: {raw_value!r}")
    value = raw_value.strip().lower()
    if value in {"off", "strict", "auto"}:
        return value
    raise CliRuntimeValidationError(f"Invalid ocr_mode for {context}: {raw_value!r}")


def _coerce_engine(raw_value: object, context: str) -> str:
    if not isinstance(raw_value, str):
        raise CliRuntimeValidationError(f"Invalid ocr_engine for {context}: {raw_value!r}")
    value = raw_value.strip().lower()
    if value == "rapidocr":
        return value
    raise CliRuntimeValidationError(f"Invalid ocr_engine for {context}: {raw_value!r}")


def _coerce_layout(raw_value: object, context: str) -> str:
    if not isinstance(raw_value, str):
        raise CliRuntimeValidationError(f"Invalid ocr_layout for {context}: {raw_value!r}")
    value = raw_value.strip().lower()
    if value in {"auto", "vertical", "horizontal"}:
        return value
    raise CliRuntimeValidationError(f"Invalid ocr_layout for {context}: {raw_value!r}")


def _coerce_zh_script(raw_value: object, context: str) -> str:
    if not isinstance(raw_value, str):
        raise CliRuntimeValidationError(f"Invalid zh_script for {context}: {raw_value!r}")
    value = raw_value.strip().lower()
    if value in {"keep", "hant", "hans"}:
        return value
    raise CliRuntimeValidationError(f"Invalid zh_script for {context}: {raw_value!r}")


def _normalize_option_value(key: str, raw_value: object, context: str) -> object:
    if key in {"output", "profile"}:
        if not isinstance(raw_value, str):
            raise CliRuntimeValidationError(f"Invalid {key} for {context}: {raw_value!r}")
        value = raw_value.strip()
        if not value:
            raise CliRuntimeValidationError(f"Invalid {key} for {context}: {raw_value!r}")
        return value
    if key in {"force", "classical_zh_postprocess", "key_content_fallback"}:
        return _coerce_bool(raw_value, context)
    if key == "split_preset":
        if isinstance(raw_value, bool):
            raise CliRuntimeValidationError(
                f"Invalid split_preset for {context}: {raw_value!r}"
            )
        if isinstance(raw_value, str):
            normalized = raw_value.strip()
        elif isinstance(raw_value, int):
            normalized = str(raw_value)
        else:
            raise CliRuntimeValidationError(
                f"Invalid split_preset for {context}: {raw_value!r}"
            )
        if not normalized.isdigit():
            raise CliRuntimeValidationError(
                f"Invalid split_preset for {context}: {raw_value!r}"
            )
        parsed = int(normalized)
        if parsed not in pdf_to_md.SPLIT_PRESET_CHOICES:
            raise CliRuntimeValidationError(
                f"Invalid split_preset for {context}: {raw_value!r}"
            )
        return parsed
    if key == "split_every":
        if isinstance(raw_value, bool):
            raise CliRuntimeValidationError(
                f"Invalid split_every for {context}: {raw_value!r}"
            )
        if isinstance(raw_value, str):
            normalized = raw_value.strip()
        elif isinstance(raw_value, int):
            normalized = str(raw_value)
        else:
            raise CliRuntimeValidationError(
                f"Invalid split_every for {context}: {raw_value!r}"
            )
        if not normalized.isdigit():
            raise CliRuntimeValidationError(
                f"Invalid split_every for {context}: {raw_value!r}"
            )
        parsed = int(normalized)
        if parsed <= 0:
            raise CliRuntimeValidationError(
                f"Invalid split_every for {context}: {raw_value!r}"
            )
        return parsed
    if key == "workers":
        if isinstance(raw_value, bool):
            raise CliRuntimeValidationError(
                f"Invalid workers for {context}: {raw_value!r}"
            )
        if isinstance(raw_value, str):
            normalized = raw_value.strip()
        elif isinstance(raw_value, int):
            normalized = str(raw_value)
        else:
            raise CliRuntimeValidationError(
                f"Invalid workers for {context}: {raw_value!r}"
            )
        if not normalized.isdigit():
            raise CliRuntimeValidationError(
                f"Invalid workers for {context}: {raw_value!r}"
            )
        parsed = int(normalized)
        if parsed <= 0:
            raise CliRuntimeValidationError(
                f"Invalid workers for {context}: {raw_value!r}"
            )
        return parsed
    if key == "ocr_mode":
        return _coerce_mode(raw_value, context)
    if key == "ocr_engine":
        return _coerce_engine(raw_value, context)
    if key == "ocr_layout":
        return _coerce_layout(raw_value, context)
    if key == "zh_script":
        return _coerce_zh_script(raw_value, context)
    raise CliRuntimeValidationError(f"Unsupported option key: {key}")


def _default_config_payload() -> dict[str, object]:
    return {
        "active_profile": "default",
        "profiles": {
            "default": {
                "force": False,
                "ocr_mode": "off",
                "ocr_engine": "rapidocr",
                "ocr_layout": "auto",
                "zh_script": "keep",
                "classical_zh_postprocess": False,
            }
        },
    }


def _resolve_config_path() -> Path:
    configured_path = os.environ.get(_CONFIG_ENV_PATH)
    if configured_path:
        return Path(configured_path).expanduser()
    return Path.cwd() / _CONFIG_DEFAULT_NAME


def _load_config_file(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except json.JSONDecodeError as error:
        raise CliRuntimeValidationError(f"Invalid config JSON at {path}: {error.msg}") from error
    if not isinstance(payload, dict):
        raise CliRuntimeValidationError(f"Config root must be an object: {path}")
    return cast(dict[str, object], payload)


def _validate_config_payload(payload: dict[str, object]) -> None:
    unknown_root_keys = sorted(set(payload.keys()) - _ROOT_ALLOWED_KEYS)
    if unknown_root_keys:
        raise CliRuntimeValidationError(
            f"Unknown config key(s): {', '.join(unknown_root_keys)}"
        )

    for option_key in _OPTION_KEYS:
        if option_key in payload:
            _ = _normalize_option_value(option_key, payload[option_key], "config")

    active_profile = payload.get("active_profile")
    if active_profile is not None:
        _ = _normalize_option_value("profile", active_profile, "config")

    profiles = payload.get("profiles")
    if profiles is None:
        return
    if not isinstance(profiles, dict):
        raise CliRuntimeValidationError("Config key 'profiles' must be an object")
    profiles_dict = cast(dict[str, object], profiles)
    for profile_name, profile_payload in profiles_dict.items():
        if not profile_name.strip():
            raise CliRuntimeValidationError(f"Invalid profile name: {profile_name!r}")
        if not isinstance(profile_payload, dict):
            raise CliRuntimeValidationError(
                f"Profile '{profile_name}' must be an object"
            )
        typed_profile_payload = cast(dict[str, object], profile_payload)
        unknown_profile_keys = sorted(set(typed_profile_payload.keys()) - _PROFILE_ALLOWED_KEYS)
        if unknown_profile_keys:
            raise CliRuntimeValidationError(
                f"Unknown profile key(s) in '{profile_name}': {', '.join(unknown_profile_keys)}"
            )
        for option_key, option_value in typed_profile_payload.items():
            _ = _normalize_option_value(option_key, option_value, f"profile:{profile_name}")


def _parse_convert_cli(argv: list[str]) -> tuple[dict[str, object], str, float]:
    parser = argparse.ArgumentParser(add_help=False)
    _ = parser.add_argument("-o", "--output")
    _ = parser.add_argument("--force", action="store_true")
    _ = parser.add_argument("--ocr-fallback", action="store_true")
    _ = parser.add_argument("--ocr")
    _ = parser.add_argument("--ocr-engine")
    _ = parser.add_argument("--ocr-layout")
    _ = parser.add_argument("--zh-script")
    _ = parser.add_argument("--ocr-classical-zh-postprocess", action="store_true")
    _ = parser.add_argument("--ocr-key-content-fallback", action="store_true")
    _ = parser.add_argument("--split-preset")
    _ = parser.add_argument("--split-every")
    _ = parser.add_argument("--workers")
    _ = parser.add_argument("--progress-compact", action="store_true")
    _ = parser.add_argument("--progress-verbose", action="store_true")
    _ = parser.add_argument("--progress-interval")
    _ = parser.add_argument("--profile")

    parsed, passthrough = parser.parse_known_args(argv)
    cli_values: dict[str, object] = {}
    output_value = cast(str | None, parsed.output)
    if output_value is not None:
        cli_values["output"] = _normalize_option_value("output", output_value, "cli")
    if cast(bool, parsed.force):
        cli_values["force"] = True
    if cast(bool, parsed.ocr_fallback):
        cli_values["ocr_mode"] = "strict"
    else:
        ocr_mode_value = cast(str | None, parsed.ocr)
        if ocr_mode_value is not None:
            cli_values["ocr_mode"] = _normalize_option_value("ocr_mode", ocr_mode_value, "cli")
    ocr_engine_value = cast(str | None, parsed.ocr_engine)
    if ocr_engine_value is not None:
        cli_values["ocr_engine"] = _normalize_option_value("ocr_engine", ocr_engine_value, "cli")
    ocr_layout_value = cast(str | None, parsed.ocr_layout)
    if ocr_layout_value is not None:
        cli_values["ocr_layout"] = _normalize_option_value("ocr_layout", ocr_layout_value, "cli")
    zh_script_value = cast(str | None, parsed.zh_script)
    if zh_script_value is not None:
        cli_values["zh_script"] = _normalize_option_value("zh_script", zh_script_value, "cli")
    if cast(bool, parsed.ocr_classical_zh_postprocess):
        cli_values["classical_zh_postprocess"] = True
    if cast(bool, parsed.ocr_key_content_fallback):
        cli_values["key_content_fallback"] = True
    split_preset_value = cast(str | None, parsed.split_preset)
    if split_preset_value is not None:
        cli_values["split_preset"] = _normalize_option_value("split_preset", split_preset_value, "cli")
    split_every_value = cast(str | None, parsed.split_every)
    if split_every_value is not None:
        cli_values["split_every"] = _normalize_option_value("split_every", split_every_value, "cli")
    workers_value = cast(str | None, parsed.workers)
    if workers_value is not None:
        cli_values["workers"] = _normalize_option_value("workers", workers_value, "cli")
    if "split_preset" in cli_values and "split_every" in cli_values:
        raise CliRuntimeValidationError("Choose only one split mode: split_preset or split_every.")
    profile_value = cast(str | None, parsed.profile)
    if profile_value is not None:
        cli_values["profile"] = _normalize_option_value("profile", profile_value, "cli")
    progress_detail = _PROGRESS_DETAIL_VERBOSE
    if cast(bool, parsed.progress_compact):
        progress_detail = _PROGRESS_DETAIL_COMPACT
    if cast(bool, parsed.progress_verbose):
        progress_detail = _PROGRESS_DETAIL_VERBOSE
    progress_interval_seconds = _PROGRESS_INTERVAL_DEFAULT_SECONDS
    progress_interval_raw = cast(str | None, parsed.progress_interval)
    if progress_interval_raw is not None:
        try:
            parsed_interval = float(progress_interval_raw)
        except ValueError as error:
            raise CliRuntimeValidationError(
                f"Invalid progress interval for cli: {progress_interval_raw!r}"
            ) from error
        if parsed_interval <= 0:
            raise CliRuntimeValidationError(
                f"Invalid progress interval for cli: {progress_interval_raw!r}"
            )
        progress_interval_seconds = max(_PROGRESS_INTERVAL_MIN_SECONDS, parsed_interval)
    _ = passthrough
    return cli_values, progress_detail, progress_interval_seconds


def _strip_wrapper_progress_flags(argv: list[str]) -> list[str]:
    stripped: list[str] = []
    skip_next = False
    for token in argv:
        if skip_next:
            skip_next = False
            continue
        if token in {"--progress-compact", "--progress-verbose"}:
            continue
        if token == "--progress-interval":
            skip_next = True
            continue
        if token.startswith("--progress-interval="):
            continue
        stripped.append(token)
    return stripped


def _parse_env_values() -> dict[str, object]:
    env_values: dict[str, object] = {}
    for key, env_key in _ENV_MAP.items():
        raw_value = os.environ.get(env_key)
        if raw_value is None:
            continue
        env_values[key] = _normalize_option_value(key, raw_value, "env")
    return env_values


def _profile_values_from_payload(payload: dict[str, object], profile_name: str | None) -> dict[str, object]:
    values: dict[str, object] = {}
    for option_key in _OPTION_KEYS:
        if option_key in payload:
            values[option_key] = _normalize_option_value(option_key, payload[option_key], "config")

    selected_profile = profile_name
    if selected_profile is None:
        active_profile = payload.get("active_profile")
        if isinstance(active_profile, str) and active_profile.strip():
            selected_profile = active_profile

    profiles_raw = payload.get("profiles")
    if selected_profile is None or profiles_raw is None:
        return values
    profiles = cast(dict[str, object], profiles_raw)
    if selected_profile not in profiles:
        raise CliRuntimeValidationError(f"Unknown profile: {selected_profile}")
    profile_payload = profiles[selected_profile]
    if not isinstance(profile_payload, dict):
        raise CliRuntimeValidationError(f"Profile '{selected_profile}' must be an object")
    typed_profile_payload = cast(dict[str, object], profile_payload)
    for option_key, option_value in typed_profile_payload.items():
        values[option_key] = _normalize_option_value(
            option_key,
            option_value,
            f"profile:{selected_profile}",
        )
    return values


def _resolve_effective_values(
    *,
    cli_values: dict[str, object],
    env_values: dict[str, object],
    file_values: dict[str, object],
) -> tuple[dict[str, object], dict[str, str]]:
    resolved: dict[str, object] = {}
    source: dict[str, str] = {}

    for option_key in _OPTION_KEYS:
        if option_key in cli_values:
            resolved[option_key] = cli_values[option_key]
            source[option_key] = "cli"
            continue
        if option_key in env_values:
            resolved[option_key] = env_values[option_key]
            source[option_key] = "env"
            continue
        if option_key in file_values:
            resolved[option_key] = file_values[option_key]
            source[option_key] = "profile"
            continue
        if option_key in _DEFAULTS:
            resolved[option_key] = _DEFAULTS[option_key]
            source[option_key] = "default"
            continue
        resolved[option_key] = None
        source[option_key] = "default"

    split_preset_source = source["split_preset"]
    split_every_source = source["split_every"]
    split_preset_value = resolved["split_preset"]
    split_every_value = resolved["split_every"]
    if split_preset_value is not None and split_every_value is not None:
        split_source_rank = {"default": 0, "profile": 1, "env": 2, "cli": 3}
        preset_rank = split_source_rank[split_preset_source]
        every_rank = split_source_rank[split_every_source]
        if preset_rank > every_rank:
            resolved["split_every"] = None
            source["split_every"] = "default"
        elif every_rank > preset_rank:
            resolved["split_preset"] = None
            source["split_preset"] = "default"
        else:
            raise CliRuntimeValidationError(
                "Choose only one split mode: split_preset or split_every."
            )

    return resolved, source


def _augment_legacy_argv_from_effective(
    *,
    original_argv: list[str],
    cli_values: dict[str, object],
    resolved: dict[str, object],
) -> list[str]:
    parser = argparse.ArgumentParser(add_help=False)
    _ = parser.add_argument("--profile")
    parsed, passthrough = parser.parse_known_args(original_argv)
    _ = parsed
    final_argv = list(passthrough)

    if "output" not in cli_values:
        output_value = resolved.get("output")
        if isinstance(output_value, str):
            final_argv.extend(["-o", output_value])

    if "force" not in cli_values and bool(resolved.get("force", False)):
        final_argv.append("--force")

    ocr_mode = cast(str, resolved["ocr_mode"])
    if "ocr_mode" not in cli_values:
        if ocr_mode == "strict":
            final_argv.append("--ocr-fallback")
        elif ocr_mode == "auto":
            final_argv.extend(["--ocr", "auto"])

    if ocr_mode in {"strict", "auto"}:
        if "ocr_engine" not in cli_values:
            final_argv.extend(["--ocr-engine", cast(str, resolved["ocr_engine"])])
        if "ocr_layout" not in cli_values:
            final_argv.extend(["--ocr-layout", cast(str, resolved["ocr_layout"])])

    zh_script = cast(str, resolved.get("zh_script", "keep"))
    if zh_script != "keep":
        final_argv.extend(["--zh-script", zh_script])

    if (
        "classical_zh_postprocess" not in cli_values
        and bool(resolved.get("classical_zh_postprocess", False))
    ):
        final_argv.append("--ocr-classical-zh-postprocess")

    if (
        "key_content_fallback" not in cli_values
        and bool(resolved.get("key_content_fallback", False))
    ):
        final_argv.append("--ocr-key-content-fallback")

    if "split_preset" not in cli_values:
        split_preset_value = resolved.get("split_preset")
        if isinstance(split_preset_value, int):
            final_argv.extend(["--split-preset", str(split_preset_value)])
    if "split_every" not in cli_values:
        split_every_value = resolved.get("split_every")
        if isinstance(split_every_value, int):
            final_argv.extend(["--split-every", str(split_every_value)])
    if "workers" not in cli_values:
        workers_value = resolved.get("workers")
        if isinstance(workers_value, int):
            final_argv.extend(["--workers", str(workers_value)])

    return final_argv


def _build_effective_state(
    *, cli_values: dict[str, object], selected_profile: str | None
) -> tuple[Path, dict[str, object], dict[str, str], str | None]:
    config_path = _resolve_config_path()
    payload = _load_config_file(config_path)
    _validate_config_payload(payload)
    env_values = _parse_env_values()

    resolved_profile = selected_profile
    if resolved_profile is None and "profile" in cli_values:
        resolved_profile = cast(str, cli_values["profile"])
    if resolved_profile is None and "profile" in env_values:
        resolved_profile = cast(str, env_values["profile"])

    file_values = _profile_values_from_payload(payload, resolved_profile)
    resolved, source = _resolve_effective_values(
        cli_values=cli_values,
        env_values=env_values,
        file_values=file_values,
    )
    if resolved_profile is None:
        active = payload.get("active_profile")
        if isinstance(active, str) and active.strip():
            resolved_profile = active
    return config_path, resolved, source, resolved_profile


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pdftomd",
        description="Hybrid CLI for PDF to Markdown conversion.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert_parser = subparsers.add_parser(
        "convert",
        add_help=False,
        help="Run conversion using the legacy conversion engine.",
        description="Forward arguments to the legacy conversion engine.",
    )
    _ = convert_parser

    init_parser = subparsers.add_parser(
        "init",
        help="Initialize CLI config/profile scaffold.",
    )
    _ = init_parser.add_argument("--force", action="store_true", help="Overwrite existing config file.")
    _ = init_parser.add_argument("--path", help="Target config file path.")

    config_parser = subparsers.add_parser(
        "config",
        help="Inspect or validate effective CLI configuration.",
    )
    config_subparsers = config_parser.add_subparsers(dest="config_command")
    config_show = config_subparsers.add_parser("show", help="Show effective config with source trace.")
    _ = config_show.add_argument("--profile")
    _ = config_show.add_argument("-o", "--output")
    _ = config_show.add_argument("--force", action="store_true")
    _ = config_show.add_argument("--ocr-fallback", action="store_true")
    _ = config_show.add_argument("--ocr")
    _ = config_show.add_argument("--ocr-engine")
    _ = config_show.add_argument("--ocr-layout")
    _ = config_show.add_argument("--zh-script")
    _ = config_show.add_argument("--ocr-classical-zh-postprocess", action="store_true")
    _ = config_show.add_argument("--ocr-key-content-fallback", action="store_true")
    _ = config_show.add_argument("--split-preset")
    _ = config_show.add_argument("--split-every")
    _ = config_show.add_argument("--workers")
    _ = config_show.add_argument("--path", help="Config file path override.")
    config_validate = config_subparsers.add_parser(
        "validate", help="Validate config/profile keys and values."
    )
    _ = config_validate.add_argument("--path", help="Config file path override.")

    profile_parser = subparsers.add_parser(
        "profile",
        help="Manage named CLI profiles.",
    )
    profile_subparsers = profile_parser.add_subparsers(dest="profile_command", required=True)
    _ = profile_subparsers.add_parser("list", help="List available profiles.")

    profile_show = profile_subparsers.add_parser("show", help="Show a profile.")
    _ = profile_show.add_argument("name", nargs="?")

    profile_use = profile_subparsers.add_parser("use", help="Set active profile.")
    _ = profile_use.add_argument("name")

    profile_set = profile_subparsers.add_parser("set", help="Set a profile key/value.")
    _ = profile_set.add_argument("name")
    _ = profile_set.add_argument("key")
    _ = profile_set.add_argument("value")

    return parser


def _is_interactive_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _prompt_text(prompt: str, default: str | None = None) -> str:
    raw_value = input(prompt).strip()
    if raw_value:
        return raw_value
    return default or ""


def _prompt_yes_no(prompt: str, *, default: bool) -> bool:
    while True:
        selected = input(prompt).strip().lower()
        if not selected:
            return default
        if selected in ("y", "yes"):
            return True
        if selected in ("n", "no"):
            return False
        _write_stderr_line("Invalid selection. Enter yes/y or no/n.")


def _prompt_numbered_choice(
    prompt: str,
    options: list[str],
    *,
    default_index: int,
) -> int:
    if not options:
        raise CliRuntimeValidationError("No selectable options available")
    if default_index < 1 or default_index > len(options):
        raise CliRuntimeValidationError("Invalid default index for selection")

    for index, label in enumerate(options, start=1):
        _ = sys.stdout.write(f"[{index}] {label}\n")

    while True:
        selected = _prompt_text(f"{prompt} [default: {default_index}]: ")
        if not selected:
            return default_index - 1
        if selected.isdigit():
            selected_index = int(selected)
            if 1 <= selected_index <= len(options):
                return selected_index - 1
        _write_stderr_line(f"Invalid selection '{selected}'. Choose 1-{len(options)}.")


def _discover_resource_pdfs() -> list[Path]:
    project_root = Path(__file__).resolve().parents[1]
    resource_dir = project_root / _RESOURCE_DIR_NAME
    if not resource_dir.exists() or not resource_dir.is_dir():
        return []
    return sorted(
        [path for path in resource_dir.iterdir() if path.is_file() and path.suffix.lower() == ".pdf"],
        key=lambda path: path.name.lower(),
    )


def _resolve_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_default_output_arg(input_pdf: Path) -> str:
    output_dir = _resolve_project_root() / _DEFAULT_DOWNLOADS_DIR_NAME / input_pdf.stem
    return str(output_dir / f"{input_pdf.stem}.md")


def _resolve_chunk_output_base(output_path: Path, input_pdf: Path) -> Path:
    if output_path.exists() and output_path.is_dir():
        return output_path / input_pdf.stem
    if output_path.suffix.lower() == ".md":
        return output_path.with_suffix("")
    return output_path


def _is_module_available(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _resolve_missing_ocr_runtime_requirements(engine: str) -> list[str]:
    missing: list[str] = []
    if engine == "rapidocr":
        if not _is_module_available("pdf2image"):
            missing.append("python module `pdf2image`")
        if not _is_module_available("rapidocr_onnxruntime"):
            missing.append("python module `rapidocr_onnxruntime`")
        if shutil.which("pdftoppm") is None:
            missing.append("system binary `pdftoppm` (install poppler-utils)")
        return missing

    return missing


def _resolve_interactive_ocr_mode_after_dependency_check(
    *,
    ocr_mode: str,
    ocr_engine: str,
) -> str:
    if ocr_mode not in {"strict", "auto"}:
        return ocr_mode

    missing_requirements = _resolve_missing_ocr_runtime_requirements(ocr_engine)
    if not missing_requirements:
        return ocr_mode

    _write_stderr_line("Selected OCR configuration is missing required dependencies:")
    for requirement in missing_requirements:
        _write_stderr_line(f"- {requirement}")

    selected_action_index = _prompt_numbered_choice(
        "Select action",
        [
            "Abort and install dependencies first",
            "Continue with OCR auto mode (OCR may be skipped; output can be headers-only)",
            "Continue with OCR off (native extraction only)",
        ],
        default_index=1,
    )
    selected_action = [
        _OCR_DEPENDENCY_ACTION_ABORT,
        _OCR_DEPENDENCY_ACTION_AUTO,
        _OCR_DEPENDENCY_ACTION_OFF,
    ][selected_action_index]

    if selected_action == _OCR_DEPENDENCY_ACTION_ABORT:
        raise CliRuntimeValidationError("Aborted interactive run due to missing OCR dependencies")
    if selected_action == _OCR_DEPENDENCY_ACTION_OFF:
        return "off"
    _write_stderr_line(
        "Proceeding with OCR auto mode without required dependencies. "
        + "If no native text exists, output may contain page headers only."
    )
    return "auto"


def _resolve_generated_chunk_paths(*, output_arg: str, input_pdf: Path) -> list[Path]:
    output_path = Path(output_arg)
    base_path = _resolve_chunk_output_base(output_path, input_pdf)
    pattern = f"{base_path.name}_p*.md"
    return sorted(base_path.parent.glob(pattern))


def _resolve_chunk_versions(*, output_arg: str, input_pdf: Path) -> dict[Path, int]:
    versions: dict[Path, int] = {}
    for chunk_path in _resolve_generated_chunk_paths(output_arg=output_arg, input_pdf=input_pdf):
        try:
            versions[chunk_path] = chunk_path.stat().st_mtime_ns
        except FileNotFoundError:
            continue
    return versions


def _collect_new_chunk_paths(
    *,
    output_arg: str,
    input_pdf: Path,
    baseline_versions: dict[Path, int],
    seen_chunks: set[Path],
) -> list[Path]:
    new_chunks: list[Path] = []
    current_versions = _resolve_chunk_versions(output_arg=output_arg, input_pdf=input_pdf)
    for chunk_path in sorted(current_versions):
        if chunk_path in seen_chunks:
            continue
        current_version = current_versions[chunk_path]
        baseline_version = baseline_versions.get(chunk_path)
        if baseline_version is not None and current_version <= baseline_version:
            continue
        seen_chunks.add(chunk_path)
        new_chunks.append(chunk_path)
    return new_chunks


def _write_chunk_bundle(*, output_arg: str, input_pdf: Path, chunk_paths: list[Path]) -> Path:
    if not chunk_paths:
        raise CliRuntimeValidationError("No chunk files found to bundle")

    output_path = Path(output_arg)
    base_path = _resolve_chunk_output_base(output_path, input_pdf)
    bundle_path = base_path.with_name(f"{base_path.name}_bundle.zip")
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for chunk_path in chunk_paths:
            archive.write(chunk_path, arcname=chunk_path.name)
    return bundle_path


def _run_interactive_no_arg_launcher() -> int:
    if not _is_interactive_tty():
        _write_stderr_line(
            "No-arg interactive mode requires a TTY. Re-run in a terminal or pass explicit subcommands."
        )
        return 1

    mode_options = [
        "Convert PDF from resource list",
        "Initialize config file",
        "Show effective config",
        "List profiles",
        "Exit",
    ]
    selected_mode_index = _prompt_numbered_choice(
        "Select mode",
        mode_options,
        default_index=1,
    )
    if selected_mode_index == 1:
        return _handle_init_command(argparse.Namespace(force=False, path=None))
    if selected_mode_index == 2:
        return _handle_config_command(argparse.Namespace(config_command="show", path=None))
    if selected_mode_index == 3:
        return _handle_profile_command(argparse.Namespace(profile_command="list"))
    if selected_mode_index == 4:
        return 0

    resource_pdfs = _discover_resource_pdfs()
    if not resource_pdfs:
        _write_stderr_line("No PDF files found in resource/. Add PDFs and re-run.")
        return 1

    file_selection_options = [
        f"Process all files in resource/ ({len(resource_pdfs)} files)",
        *[pdf_path.name for pdf_path in resource_pdfs],
    ]
    selected_pdf_index = _prompt_numbered_choice(
        "Select PDF or batch mode",
        file_selection_options,
        default_index=2,
    )
    selected_pdfs: list[Path]
    output_by_pdf: dict[Path, str]
    if selected_pdf_index == 0:
        selected_pdfs = list(resource_pdfs)
        default_output_root = _resolve_project_root() / _DEFAULT_DOWNLOADS_DIR_NAME
        output_root_raw = _prompt_text(
            f"Output base directory for all files [{default_output_root}]: ",
            str(default_output_root),
        )
        output_root = Path(output_root_raw).expanduser()
        output_by_pdf = {
            pdf_path: str(output_root / pdf_path.stem / f"{pdf_path.stem}.md")
            for pdf_path in selected_pdfs
        }
    else:
        selected_pdf = resource_pdfs[selected_pdf_index - 1]
        selected_pdfs = [selected_pdf]
        default_output = _resolve_default_output_arg(selected_pdf)
        output_value = _prompt_text(
            f"Output markdown path [{default_output}]: ",
            default_output,
        )
        output_by_pdf = {selected_pdf: output_value}

    force_enabled = _prompt_yes_no("Overwrite output if exists? [Y/n]: ", default=True)

    ocr_mode_index = _prompt_numbered_choice(
        "Select OCR mode",
        ["off", "strict", "auto"],
        default_index=3,
    )
    ocr_mode = ["off", "strict", "auto"][ocr_mode_index]

    ocr_engine = "rapidocr"
    ocr_layout = "auto"
    if ocr_mode in {"strict", "auto"}:
        _write_stderr_line("OCR engine is fixed to rapidocr.")
        layout_index = _prompt_numbered_choice(
            "Select OCR layout",
            ["auto", "vertical", "horizontal"],
            default_index=1,
        )
        ocr_layout = ["auto", "vertical", "horizontal"][layout_index]

    ocr_mode = _resolve_interactive_ocr_mode_after_dependency_check(
        ocr_mode=ocr_mode,
        ocr_engine=ocr_engine,
    )

    classical_postprocess = _prompt_yes_no(
        "Enable classical Chinese postprocess? [y/N]: ",
        default=False,
    )
    key_content_fallback = _prompt_yes_no(
        "Enable key-content fallback? [y/N]: ",
        default=False,
    )

    split_mode_index = _prompt_numbered_choice(
        "Select split mode",
        ["No split", "Split by preset", "Split every N pages"],
        default_index=1,
    )
    split_args: list[str] = []
    split_selected = False
    if split_mode_index == 1:
        preset_choice_index = _prompt_numbered_choice(
            "Select split preset",
            [str(value) for value in pdf_to_md.SPLIT_PRESET_CHOICES],
            default_index=1,
        )
        split_args = ["--split-preset", str(pdf_to_md.SPLIT_PRESET_CHOICES[preset_choice_index])]
        split_selected = True
    elif split_mode_index == 2:
        split_every_raw = _prompt_text("Split every N pages [default: 10]: ", "10")
        if not split_every_raw.isdigit() or int(split_every_raw) <= 0:
            raise CliRuntimeValidationError(f"Invalid split_every in interactive mode: {split_every_raw!r}")
        split_args = ["--split-every", split_every_raw]
        split_selected = True

    delivery_mode = _DELIVERY_IMMEDIATE
    if split_selected:
        delivery_index = _prompt_numbered_choice(
            "Select split file delivery mode",
            [
                "Immediate: each split file is available as soon as generated",
                "Batch: bundle all split files into one zip after completion",
            ],
            default_index=1,
        )
        delivery_mode = _DELIVERY_IMMEDIATE if delivery_index == 0 else _DELIVERY_BATCH

    if len(selected_pdfs) == 1:
        _write_stderr_line("Starting conversion with interactive selection...")
    else:
        _write_stderr_line(
            f"Starting batch conversion for {len(selected_pdfs)} files from resource/..."
        )

    global _live_monitor_context
    failed_files: list[tuple[Path, int]] = []
    for pdf_path in selected_pdfs:
        output_value = output_by_pdf[pdf_path]
        convert_argv: list[str] = [str(pdf_path), "-o", output_value]
        if force_enabled:
            convert_argv.append("--force")
        if ocr_mode == "strict":
            convert_argv.append("--ocr-fallback")
        elif ocr_mode == "auto":
            convert_argv.extend(["--ocr", "auto"])
        if ocr_mode in {"strict", "auto"}:
            convert_argv.extend(["--ocr-engine", ocr_engine, "--ocr-layout", ocr_layout])
        if classical_postprocess:
            convert_argv.append("--ocr-classical-zh-postprocess")
        if key_content_fallback:
            convert_argv.append("--ocr-key-content-fallback")
        convert_argv.extend(split_args)

        _write_stderr_line(f"Converting: {pdf_path.name}")
        previous_context = _live_monitor_context
        _live_monitor_context = _LiveMonitorContext(
            input_pdf=pdf_path,
            output_arg=output_value,
            split_selected=split_selected,
            delivery_mode=delivery_mode,
            execution_mode_hint=_infer_execution_mode_hint(convert_argv),
            progress_detail=_PROGRESS_DETAIL_VERBOSE,
            progress_interval_seconds=_PROGRESS_INTERVAL_DEFAULT_SECONDS,
        )
        try:
            exit_code = _invoke_legacy_main(convert_argv)
        finally:
            _live_monitor_context = previous_context

        if exit_code != 0:
            failed_files.append((pdf_path, exit_code))
            _write_stderr_line(f"Failed: {pdf_path} (exit_code={exit_code})")
            continue

        if split_selected:
            chunk_paths = _resolve_generated_chunk_paths(
                output_arg=output_value,
                input_pdf=pdf_path,
            )
            if delivery_mode == _DELIVERY_BATCH:
                bundle_path = _write_chunk_bundle(
                    output_arg=output_value,
                    input_pdf=pdf_path,
                    chunk_paths=chunk_paths,
                )
                _write_stderr_line(
                    f"Batch bundle ready for {pdf_path.name}: {bundle_path} (chunk_count={len(chunk_paths)})"
                )
            else:
                _write_stderr_line(
                    f"Split files ready immediately for {pdf_path.name} (count={len(chunk_paths)}):"
                )
                for chunk_path in chunk_paths:
                    _write_stderr_line(f"- {chunk_path}")

    if failed_files:
        _write_stderr_line("Batch conversion completed with failures:")
        for failed_pdf, code in failed_files:
            _write_stderr_line(f"- {failed_pdf}: exit_code={code}")
        if len(selected_pdfs) == 1:
            return failed_files[0][1]
        return 1

    return 0


def _write_stderr_line(message: str) -> None:
    _ = sys.stderr.write(f"{message}\n")


def _parse_int_token(value: str) -> int | None:
    normalized = value.strip().rstrip(",;").rstrip("%")
    if not normalized:
        return None
    if normalized.isdigit() or (normalized.startswith("-") and normalized[1:].isdigit()):
        return int(normalized)
    return None


def _parse_jsonl_progress_event(line: str) -> tuple[int, str] | None:
    normalized_line = line.strip()
    if not normalized_line.startswith("{"):
        return None

    try:
        payload_raw = cast(object, json.loads(normalized_line))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload_raw, dict):
        return None
    payload = cast(dict[str, object], payload_raw)

    stage_raw = payload.get("stage")
    if not isinstance(stage_raw, str) or not stage_raw.strip():
        return None

    percent_raw = payload.get("percent")
    percent_value: int | None = None
    if isinstance(percent_raw, bool):
        return None
    if isinstance(percent_raw, int):
        percent_value = percent_raw
    elif isinstance(percent_raw, float):
        percent_value = int(percent_raw)
    elif isinstance(percent_raw, str):
        percent_value = _parse_int_token(percent_raw)
    if percent_value is None:
        return None

    return percent_value, stage_raw


def _parse_progress_line(line: str) -> tuple[int, str] | None:
    jsonl_event = _parse_jsonl_progress_event(line)
    if jsonl_event is not None:
        return jsonl_event

    matched = _PROGRESS_LINE_PATTERN.match(line)
    if matched is None:
        return None

    percent = _parse_int_token(matched.group("percent"))
    if percent is None:
        return None
    return percent, matched.group("stage")


def _split_stage_tokens(stage_text: str) -> tuple[str, dict[str, str]]:
    stage_label_parts: list[str] = []
    token_values: dict[str, str] = {}
    for part in stage_text.split():
        if "=" not in part:
            stage_label_parts.append(part)
            continue
        key, value = part.split("=", 1)
        normalized_key = key.strip().lower()
        normalized_value = value.strip().rstrip(",;")
        if normalized_key:
            token_values[normalized_key] = normalized_value
    stage_label = " ".join(stage_label_parts).strip() if stage_label_parts else stage_text.strip()
    return stage_label, token_values


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


def _current_cpu_usage_ratio() -> float | None:
    cpu_count = os.cpu_count() or 1
    try:
        load_avg = os.getloadavg()[0]
    except Exception:
        return None
    return max(0.0, load_avg / max(1, cpu_count))


def _current_memory_usage_ratio() -> float | None:
    memory_info = _read_meminfo_bytes()
    if memory_info is None:
        return None
    total_bytes, available_bytes = memory_info
    used_ratio = 1.0 - (available_bytes / total_bytes)
    return max(0.0, min(1.0, used_ratio))


def _format_seconds(seconds: float) -> str:
    bounded = max(0, int(seconds))
    hours = bounded // 3600
    minutes = (bounded % 3600) // 60
    secs = bounded % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _infer_execution_mode_hint(argv: list[str]) -> str:
    has_split = "--split-preset" in argv or "--split-every" in argv
    has_ocr = "--ocr-fallback" in argv or ("--ocr" in argv and "auto" in argv)
    if has_split and has_ocr:
        return "serial chunks (split-before-ocr)"
    if has_split and not has_ocr:
        return "parallel chunks (thread pool)"
    return "parallel render (thread pool)"


def _resolve_output_arg(argv: list[str], input_pdf: Path) -> str:
    for index, token in enumerate(argv):
        if token == "-o" and index + 1 < len(argv):
            return argv[index + 1]
        if token.startswith("--output="):
            return token.split("=", 1)[1]
        if token == "--output" and index + 1 < len(argv):
            return argv[index + 1]
    return _resolve_default_output_arg(input_pdf)


def _build_live_monitor_context_from_argv(
    argv: list[str],
    *,
    delivery_mode: str,
    progress_detail: str,
    progress_interval_seconds: float,
) -> _LiveMonitorContext | None:
    if not argv:
        return None
    input_token = argv[0]
    if input_token.startswith("-"):
        return None
    input_pdf = Path(input_token)
    output_arg = _resolve_output_arg(argv, input_pdf)
    split_selected = "--split-preset" in argv or "--split-every" in argv
    return _LiveMonitorContext(
        input_pdf=input_pdf,
        output_arg=output_arg,
        split_selected=split_selected,
        delivery_mode=delivery_mode,
        execution_mode_hint=_infer_execution_mode_hint(argv),
        progress_detail=progress_detail,
        progress_interval_seconds=progress_interval_seconds,
    )


def _resolve_converter_script_path() -> Path:
    module_path = getattr(pdf_to_md, "__file__", None)
    if isinstance(module_path, str):
        return Path(module_path).resolve()
    return Path(__file__).resolve().with_name("pdf_to_md.py")


def _emit_live_status_line(
    *,
    start_time: float,
    percent: int,
    stage_label: str,
    token_values: dict[str, str],
    context: _LiveMonitorContext,
    speed_text_override: str | None = None,
) -> None:
    now = time.monotonic()
    elapsed = max(0.0, now - start_time)
    eta_seconds: float | None = None
    if percent > 0:
        eta_seconds = elapsed * (100 - percent) / percent

    page_current = _parse_int_token(token_values.get("current", ""))
    page_total = _parse_int_token(token_values.get("total", ""))
    page_remaining = _parse_int_token(token_values.get("remaining", ""))
    page_percent = _parse_int_token(token_values.get("page_percent", ""))
    chunk_index = _parse_int_token(token_values.get("chunk_index", ""))
    chunk_total = _parse_int_token(token_values.get("chunk_total", ""))

    speed_text = "n/a"
    if speed_text_override:
        speed_text = speed_text_override
    elif elapsed > 0 and page_current is not None and page_current > 0:
        speed_text = f"{page_current / elapsed:.2f} pages/s"
    elif elapsed > 0 and chunk_index is not None and chunk_index > 0:
        speed_text = f"{chunk_index / elapsed:.2f} chunks/s"

    cpu_ratio = _current_cpu_usage_ratio()
    mem_ratio = _current_memory_usage_ratio()
    cpu_text = f"{cpu_ratio * 100:.0f}%" if cpu_ratio is not None else "n/a"
    mem_text = f"{mem_ratio * 100:.0f}%" if mem_ratio is not None else "n/a"

    page_progress_text = "n/a"
    if page_current is not None and page_total is not None:
        page_progress_text = f"{page_current}/{page_total}"
    if page_percent is not None:
        page_progress_text = f"{page_progress_text} ({page_percent}%)"
    if page_remaining is not None:
        page_progress_text = f"{page_progress_text}, remaining={page_remaining}"

    chunk_progress_text = "n/a"
    if chunk_index is not None and chunk_total is not None:
        chunk_progress_text = f"{chunk_index}/{chunk_total}"

    eta_text = _format_seconds(eta_seconds) if eta_seconds is not None else "n/a"
    if context.progress_detail == _PROGRESS_DETAIL_COMPACT:
        _write_stderr_line(
            "Live: "
            + f"file={context.input_pdf.name} "
            + f"overall={percent}% "
            + f"stage='{stage_label}' "
            + f"eta={eta_text} "
            + f"speed={speed_text}"
        )
        return

    _write_stderr_line(
        "Live: "
        + f"file={context.input_pdf.name} "
        + f"overall={percent}% "
        + f"stage='{stage_label}' "
        + f"page={page_progress_text} "
        + f"chunk={chunk_progress_text} "
        + f"elapsed={_format_seconds(elapsed)} eta={eta_text} "
        + f"speed={speed_text} "
        + f"cpu_load={cpu_text} mem_load={mem_text} "
        + f"mode={context.execution_mode_hint}"
    )


def _invoke_legacy_main_with_live_monitor(argv: list[str], context: _LiveMonitorContext) -> int:
    command = [sys.executable, str(_resolve_converter_script_path()), *argv]
    cpu_cores = os.cpu_count() or 1
    _write_stderr_line(
        "Live monitor: started "
        + f"file={context.input_pdf.name} mode={context.execution_mode_hint} cpu_cores={cpu_cores}"
    )

    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    def _terminate_child_process(*, reason: str) -> None:
        if process.poll() is not None:
            return
        _write_stderr_line(f"Live monitor: terminating child process ({reason})")
        process.terminate()
        try:
            _ = process.wait(timeout=_LIVE_MONITOR_TERMINATE_GRACE_SECONDS)
            return
        except subprocess.TimeoutExpired:
            pass
        _write_stderr_line("Live monitor: child did not terminate in grace period; killing")
        process.kill()
        _ = process.wait()

    watchdog_timer = threading.Timer(
        _LIVE_MONITOR_MAX_SECONDS,
        _terminate_child_process,
        kwargs={"reason": f"watchdog timeout={int(_LIVE_MONITOR_MAX_SECONDS)}s"},
    )
    watchdog_timer.daemon = True
    watchdog_timer.start()

    baseline_chunk_versions: dict[Path, int] = {}
    if context.split_selected and context.delivery_mode == _DELIVERY_IMMEDIATE:
        baseline_chunk_versions = _resolve_chunk_versions(
            output_arg=context.output_arg,
            input_pdf=context.input_pdf,
        )
    seen_chunks: set[Path] = set()
    start_time = time.monotonic()
    last_live_emit_time = 0.0
    ewma_rate_by_kind: dict[str, float | None] = {"page": None, "chunk": None}
    last_counter_value_by_kind: dict[str, int | None] = {"page": None, "chunk": None}
    last_counter_time_by_kind: dict[str, float | None] = {"page": None, "chunk": None}
    ewma_unit = "items/s"
    exit_code = 1
    try:
        stderr_stream = process.stderr
        if stderr_stream is not None:
            for raw_line in stderr_stream:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue

                _write_stderr_line(line)

                parsed_progress = _parse_progress_line(line)
                if parsed_progress is not None:
                    now = time.monotonic()
                    percent, stage_text = parsed_progress
                    bounded_percent = max(0, min(100, percent))
                    stage_label, token_values = _split_stage_tokens(stage_text)

                    counter_value: int | None = None
                    counter_kind_now: str | None = None
                    page_counter = _parse_int_token(token_values.get("current", ""))
                    chunk_counter = _parse_int_token(token_values.get("chunk_index", ""))
                    if page_counter is not None:
                        counter_value = page_counter
                        counter_kind_now = "page"
                        ewma_unit = "pages/s"
                    elif chunk_counter is not None:
                        counter_value = chunk_counter
                        counter_kind_now = "chunk"
                        ewma_unit = "chunks/s"

                    active_ewma_rate: float | None = None
                    if counter_kind_now is not None:
                        kind = counter_kind_now
                        previous_counter_value = last_counter_value_by_kind[kind]
                        previous_counter_time = last_counter_time_by_kind[kind]
                        if (
                            counter_value is not None
                            and previous_counter_value is not None
                            and previous_counter_time is not None
                            and counter_value > previous_counter_value
                        ):
                            delta_value = counter_value - previous_counter_value
                            delta_time = max(1e-6, now - previous_counter_time)
                            instant_rate = delta_value / delta_time
                            current_ewma_rate = ewma_rate_by_kind[kind]
                            if current_ewma_rate is None:
                                ewma_rate_by_kind[kind] = instant_rate
                            else:
                                ewma_rate_by_kind[kind] = (
                                    0.35 * instant_rate + 0.65 * current_ewma_rate
                                )

                        active_ewma_rate = ewma_rate_by_kind[kind]
                        if counter_value is not None:
                            last_counter_value_by_kind[kind] = counter_value
                            last_counter_time_by_kind[kind] = now

                    should_emit = (
                        bounded_percent >= 100
                        or (last_live_emit_time <= 0.0)
                        or (now - last_live_emit_time >= context.progress_interval_seconds)
                    )
                    if should_emit:
                        speed_override = None
                        if active_ewma_rate is not None:
                            speed_override = f"{active_ewma_rate:.2f} {ewma_unit}"
                        _emit_live_status_line(
                            start_time=start_time,
                            percent=bounded_percent,
                            stage_label=stage_label,
                            token_values=token_values,
                            context=context,
                            speed_text_override=speed_override,
                        )
                        last_live_emit_time = now

                if context.split_selected and context.delivery_mode == _DELIVERY_IMMEDIATE:
                    new_chunk_paths = _collect_new_chunk_paths(
                        output_arg=context.output_arg,
                        input_pdf=context.input_pdf,
                        baseline_versions=baseline_chunk_versions,
                        seen_chunks=seen_chunks,
                    )
                    for chunk_path in new_chunk_paths:
                        _write_stderr_line(f"Live chunk ready: {chunk_path}")

        exit_code = process.wait()
    except KeyboardInterrupt:
        _terminate_child_process(reason="keyboard interrupt")
        _write_stderr_line("Live monitor: interrupted by user")
        exit_code = 130
    finally:
        watchdog_timer.cancel()
        process_poll = getattr(process, "poll", None)
        if callable(process_poll) and process_poll() is None:
            _terminate_child_process(reason="monitor cleanup")
        process_stderr = getattr(process, "stderr", None)
        if process_stderr is not None and hasattr(process_stderr, "close"):
            process_stderr.close()

    elapsed = time.monotonic() - start_time
    _write_stderr_line(
        "Live monitor: finished "
        + f"file={context.input_pdf.name} exit_code={exit_code} elapsed={_format_seconds(elapsed)}"
    )
    return exit_code


def _emit_deprecation_warning_once(
    emitted_warning_classes: set[str], warning_class: str, message: str
) -> None:
    if warning_class in emitted_warning_classes:
        return
    emitted_warning_classes.add(warning_class)
    _write_stderr_line(message)


def _adapt_legacy_invocation(argv: list[str]) -> tuple[list[str], bool]:
    if not argv:
        return argv, False

    first_token = argv[0]
    if first_token in _HYBRID_COMMANDS:
        return argv, False
    if first_token in ("-h", "--help"):
        return argv, False
    return ["convert", *argv], True


def _invoke_legacy_main(argv: list[str]) -> int:
    if _live_monitor_context is not None:
        return _invoke_legacy_main_with_live_monitor(argv, _live_monitor_context)
    try:
        return pdf_to_md.main(argv)
    except SystemExit as error:
        code = error.code
        if isinstance(code, int):
            return code
        return 1


def _write_config_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ns_str(args: argparse.Namespace, key: str) -> str | None:
    value = getattr(args, key, None)
    if value is None:
        return None
    if not isinstance(value, str):
        raise CliRuntimeValidationError(f"Invalid argument type for {key}: {value!r}")
    return value


def _ns_bool(args: argparse.Namespace, key: str) -> bool:
    value = getattr(args, key, False)
    if isinstance(value, bool):
        return value
    raise CliRuntimeValidationError(f"Invalid argument type for {key}: {value!r}")


def _handle_init_command(args: argparse.Namespace) -> int:
    path_value = _ns_str(args, "path")
    target_path = Path(path_value).expanduser() if path_value else _resolve_config_path()
    if target_path.exists() and not _ns_bool(args, "force"):
        _write_stderr_line(
            f"Config file already exists: {target_path} (use --force to overwrite)"
        )
        return 1
    _write_config_file(target_path, _default_config_payload())
    return 0


def _config_show_cli_values(args: argparse.Namespace) -> dict[str, object]:
    cli_values: dict[str, object] = {}
    output_value = _ns_str(args, "output")
    if output_value is not None:
        cli_values["output"] = _normalize_option_value("output", output_value, "cli")
    if _ns_bool(args, "force"):
        cli_values["force"] = True
    if _ns_bool(args, "ocr_fallback"):
        cli_values["ocr_mode"] = "strict"
    else:
        ocr_mode_value = _ns_str(args, "ocr")
        if ocr_mode_value is not None:
            cli_values["ocr_mode"] = _normalize_option_value("ocr_mode", ocr_mode_value, "cli")
    ocr_engine_value = _ns_str(args, "ocr_engine")
    if ocr_engine_value is not None:
        cli_values["ocr_engine"] = _normalize_option_value("ocr_engine", ocr_engine_value, "cli")
    ocr_layout_value = _ns_str(args, "ocr_layout")
    if ocr_layout_value is not None:
        cli_values["ocr_layout"] = _normalize_option_value("ocr_layout", ocr_layout_value, "cli")
    zh_script_value = _ns_str(args, "zh_script")
    if zh_script_value is not None:
        cli_values["zh_script"] = _normalize_option_value("zh_script", zh_script_value, "cli")
    if _ns_bool(args, "ocr_classical_zh_postprocess"):
        cli_values["classical_zh_postprocess"] = True
    if _ns_bool(args, "ocr_key_content_fallback"):
        cli_values["key_content_fallback"] = True
    split_preset_value = _ns_str(args, "split_preset")
    if split_preset_value is not None:
        cli_values["split_preset"] = _normalize_option_value("split_preset", split_preset_value, "cli")
    split_every_value = _ns_str(args, "split_every")
    if split_every_value is not None:
        cli_values["split_every"] = _normalize_option_value("split_every", split_every_value, "cli")
    workers_value = _ns_str(args, "workers")
    if workers_value is not None:
        cli_values["workers"] = _normalize_option_value("workers", workers_value, "cli")
    if "split_preset" in cli_values and "split_every" in cli_values:
        raise CliRuntimeValidationError("Choose only one split mode: split_preset or split_every.")
    profile_value = _ns_str(args, "profile")
    if profile_value is not None:
        cli_values["profile"] = _normalize_option_value("profile", profile_value, "cli")
    return cli_values


def _handle_config_command(args: argparse.Namespace) -> int:
    config_command = _ns_str(args, "config_command") or "show"
    path_override = _ns_str(args, "path")
    previous_config_path = os.environ.get(_CONFIG_ENV_PATH)
    if path_override:
        os.environ[_CONFIG_ENV_PATH] = path_override

    try:
        config_path = _resolve_config_path()

        if config_command == "validate":
            payload = _load_config_file(config_path)
            _validate_config_payload(payload)
            return 0

        cli_values = _config_show_cli_values(args)
        _, resolved, source, profile_name = _build_effective_state(
            cli_values=cli_values,
            selected_profile=cast(str | None, cli_values.get("profile")),
        )
        printable: dict[str, object] = {
            "config_path": str(config_path),
            "profile": profile_name,
            "effective": resolved,
            "sources": source,
        }
        _ = sys.stdout.write(json.dumps(printable, sort_keys=True) + "\n")
        return 0
    finally:
        if path_override:
            if previous_config_path is None:
                _ = os.environ.pop(_CONFIG_ENV_PATH, None)
            else:
                os.environ[_CONFIG_ENV_PATH] = previous_config_path


def _load_and_validate_for_profile() -> tuple[Path, dict[str, object]]:
    config_path = _resolve_config_path()
    payload = _load_config_file(config_path)
    _validate_config_payload(payload)
    return config_path, payload


def _ensure_profiles(payload: dict[str, object]) -> dict[str, object]:
    profiles = payload.get("profiles")
    if profiles is None:
        payload["profiles"] = {}
        profiles = payload["profiles"]
    if not isinstance(profiles, dict):
        raise CliRuntimeValidationError("Config key 'profiles' must be an object")
    return cast(dict[str, object], profiles)


def _handle_profile_command(args: argparse.Namespace) -> int:
    config_path, payload = _load_and_validate_for_profile()
    profiles = _ensure_profiles(payload)
    active_profile = payload.get("active_profile")

    profile_command = _ns_str(args, "profile_command")

    if profile_command == "list":
        for profile_name in sorted(profiles.keys()):
            marker = "*" if profile_name == active_profile else " "
            _ = sys.stdout.write(f"{marker} {profile_name}\n")
        return 0

    if profile_command == "show":
        requested_name = _ns_str(args, "name")
        if requested_name is None:
            if not isinstance(active_profile, str) or not active_profile.strip():
                raise CliRuntimeValidationError("No active profile configured")
            requested_name = active_profile
        if requested_name not in profiles:
            raise CliRuntimeValidationError(f"Unknown profile: {requested_name}")
        profile_payload = profiles[requested_name]
        if not isinstance(profile_payload, dict):
            raise CliRuntimeValidationError(f"Profile '{requested_name}' must be an object")
        printable: dict[str, object] = {
            "profile": requested_name,
            "values": cast(dict[str, object], profile_payload),
            "active": requested_name == active_profile,
        }
        _ = sys.stdout.write(json.dumps(printable, sort_keys=True) + "\n")
        return 0

    if profile_command == "use":
        requested_name = _ns_str(args, "name")
        if requested_name is None:
            raise CliRuntimeValidationError("Profile name is required")
        if requested_name not in profiles:
            raise CliRuntimeValidationError(f"Unknown profile: {requested_name}")
        payload["active_profile"] = requested_name
        _write_config_file(config_path, payload)
        return 0

    if profile_command == "set":
        profile_name = _ns_str(args, "name")
        key = _ns_str(args, "key")
        value = _ns_str(args, "value")
        if profile_name is None or key is None or value is None:
            raise CliRuntimeValidationError("profile set requires name, key, and value")
        if key not in _PROFILE_ALLOWED_KEYS:
            raise CliRuntimeValidationError(f"Unknown profile key: {key}")
        normalized_value = _normalize_option_value(key, value, "profile:set")
        profile_payload = profiles.get(profile_name)
        if profile_payload is None:
            profile_payload = {}
            profiles[profile_name] = profile_payload
        if not isinstance(profile_payload, dict):
            raise CliRuntimeValidationError(f"Profile '{profile_name}' must be an object")
        profile_payload[key] = normalized_value
        _write_config_file(config_path, payload)
        return 0

    raise CliRuntimeValidationError(f"Unknown profile command: {profile_command}")


def main(argv: list[str] | None = None) -> int:
    resolved_argv = list(sys.argv[1:] if argv is None else argv)
    if not resolved_argv:
        try:
            return _run_interactive_no_arg_launcher()
        except CliRuntimeValidationError as error:
            _write_stderr_line(str(error))
            return 1
    adapted_argv, used_legacy_adapter = _adapt_legacy_invocation(resolved_argv)
    emitted_warning_classes: set[str] = set()
    if used_legacy_adapter:
        _emit_deprecation_warning_once(
            emitted_warning_classes,
            _LEGACY_COMPAT_WARNING_CLASS,
            _LEGACY_COMPAT_WARNING_MESSAGE,
        )

    parser = _build_parser()
    args, remainder = parser.parse_known_args(adapted_argv)

    command = cast(str, args.command)
    try:
        if command == "convert":
            cli_values, progress_detail, progress_interval_seconds = _parse_convert_cli(remainder)
            _, resolved, _, selected_profile = _build_effective_state(
                cli_values=cli_values,
                selected_profile=cast(str | None, cli_values.get("profile")),
            )
            _ = selected_profile
            sanitized_remainder = _strip_wrapper_progress_flags(remainder)
            forwarded_argv = _augment_legacy_argv_from_effective(
                original_argv=sanitized_remainder,
                cli_values=cli_values,
                resolved=resolved,
            )
            global _live_monitor_context
            previous_context = _live_monitor_context
            _live_monitor_context = (
                None
                if used_legacy_adapter or not sys.stderr.isatty()
                else _build_live_monitor_context_from_argv(
                    forwarded_argv,
                    delivery_mode=_DELIVERY_IMMEDIATE,
                    progress_detail=progress_detail,
                    progress_interval_seconds=progress_interval_seconds,
                )
            )
            try:
                return _invoke_legacy_main(forwarded_argv)
            finally:
                _live_monitor_context = previous_context
        if command == "init":
            if remainder:
                parser.error(f"unrecognized arguments: {' '.join(remainder)}")
            return _handle_init_command(args)
        if command == "config":
            return _handle_config_command(args)
        if command == "profile":
            return _handle_profile_command(args)
        if remainder:
            parser.error(f"unrecognized arguments: {' '.join(remainder)}")
        return 0
    except CliRuntimeValidationError as error:
        _write_stderr_line(str(error))
        return 1


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
