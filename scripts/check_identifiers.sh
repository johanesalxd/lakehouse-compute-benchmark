#!/usr/bin/env bash
set -euo pipefail

# Public repository leak guard: scans tracked and untracked (non-gitignored)
# workspace files (or optional explicit path arguments) for internal Google/GTM
# identifiers, personal project numbers, default GCE SA emails, and optional
# caller-supplied EXTRA_FORBIDDEN_REGEX patterns.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required by scripts/check_identifiers.sh" >&2
  exit 1
fi

exec python3 - "${REPO_ROOT}" "$@" <<'PY'
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

repo_root = Path(sys.argv[1]).resolve()
cli_targets = [Path(arg).resolve() for arg in sys.argv[2:]]

_AT = "@"
_DOT = r"\."

FORBIDDEN_SPECS: list[tuple[str, re.Pattern[str]]] = [
    ("corp_email", re.compile(_AT + "goo" + "gle" + _DOT + "com", re.IGNORECASE)),
    ("altostrat_domain", re.compile("alto" + "strat" + _DOT + "com", re.IGNORECASE)),
    ("corp_domain", re.compile("co" + "rp" + _DOT + "google", re.IGNORECASE)),
    ("go_shortlink", re.compile(r"\b" + "g" + r"o/[a-z0-9_-]+", re.IGNORECASE)),
    ("goto_domain", re.compile("go" + "to" + _DOT + "google" + _DOT + "com", re.IGNORECASE)),
    ("depot_path", re.compile("/" + "/depot/", re.IGNORECASE)),
    ("google3_path", re.compile(r"\b" + "goo" + r"gle3/", re.IGNORECASE)),
    ("bug_or_cl_ref", re.compile(r"\b(" + "b" + "|" + "cl" + r")/[0-9]{4,}", re.IGNORECASE)),
    ("corp_chat", re.compile("ch" + "at" + _DOT + "google" + _DOT + "com", re.IGNORECASE)),
    ("cloud_gtm", re.compile("cl" + "oud-gtm", re.IGNORECASE)),
    ("gtap_term", re.compile(r"\b" + "g" + r"TAP\b")),
    ("edp_term", re.compile(r"\b" + "E" + r"DP\b")),
    ("bundle_adjustment", re.compile(r"\b" + "Bun" + r"dle Adjustment\b")),
    ("da_fast", re.compile(r"\b" + "D" + r"A FAST\b")),
    ("kcmd_term", re.compile(r"\b" + "k" + r"cmd\b")),
    (
        "default_compute_sa",
        re.compile(
            "[-][c]ompute" + _AT + "devel" + "oper" + _DOT + "gserviceaccount" + _DOT + "com",
            re.IGNORECASE,
        ),
    ),
    (
        "playground_project",
        re.compile(r"[a-z0-9-]+-" + "play" + r"ground-[0-9]{6}", re.IGNORECASE),
    ),
    ("twelve_digit_number", re.compile(r"\b[0-9]{12}\b")),
]

extra_regex = os.environ.get("EXTRA_FORBIDDEN_REGEX", "").strip()
if extra_regex:
    FORBIDDEN_SPECS.append(("extra_forbidden_regex", re.compile(extra_regex)))

SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".ipynb_checkpoints",
}

_BQ_BYTES_TD_RE = re.compile(r"^\s*<td>[0-9]{12}</td>\s*$")
_BQ_BYTES_ROW_RE = re.compile(r"^\s*[0-9]+\s+[0-9]+\s+[0-9]{12}\s+[0-9.]+\s*$")


def collect_files() -> list[Path]:
    if cli_targets:
        found: list[Path] = []
        for target in cli_targets:
            if target.is_file():
                found.append(target)
            elif target.is_dir():
                for root, dirs, names in os.walk(target):
                    dirs[:] = sorted(d for d in dirs if d not in SKIP_DIR_NAMES)
                    for name in sorted(names):
                        p = Path(root) / name
                        if p.is_file():
                            found.append(p)
        return sorted(set(found))

    proc = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "-co", "--exclude-standard"],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        files = [repo_root / line for line in proc.stdout.splitlines() if line.strip()]
        return [p for p in files if p.is_file() and not any(part in SKIP_DIR_NAMES for part in p.parts)]

    found = []
    for root, dirs, names in os.walk(repo_root):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIR_NAMES)
        for name in sorted(names):
            p = Path(root) / name
            if p.is_file():
                found.append(p)
    return sorted(found)


def is_binary_bytes(raw: bytes) -> bool:
    return b"\x00" in raw[:8192]


def scan_jsonl_telemetry(path: Path, text: str) -> list[str]:
    hits: list[str] = []
    exempt_numeric_keys = {"bq_bytes_processed", "checksum_net_paid"}
    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        for rule_name, pattern in FORBIDDEN_SPECS:
            if rule_name != "twelve_digit_number":
                if pattern.search(raw_line):
                    hits.append(f"{path}:{line_no}: [{rule_name}] match in telemetry JSONL")
                continue

        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            if FORBIDDEN_SPECS[-1][1].search(raw_line):
                hits.append(f"{path}:{line_no}: [twelve_digit_number] invalid JSON line")
            continue

        for key, val in obj.items():
            val_str = str(val)
            if isinstance(val, (int, float)) and key in exempt_numeric_keys:
                continue
            for rule_name, pattern in FORBIDDEN_SPECS:
                if pattern.search(val_str):
                    hits.append(f"{path}:{line_no}: [{rule_name}] in field '{key}'")
    return hits


def scan_notebook(path: Path, text: str) -> list[str]:
    hits: list[str] = []
    try:
        nb = json.loads(text)
    except json.JSONDecodeError:
        return scan_plain_lines(path, text)

    cells = nb.get("cells") or []
    for ci, cell in enumerate(cells):
        for si, line in enumerate(cell.get("source") or [], start=1):
            for rule_name, pattern in FORBIDDEN_SPECS:
                if pattern.search(line):
                    hits.append(f"{path}:cell[{ci}].source[{si}]: [{rule_name}]")

        for oi, out in enumerate(cell.get("outputs") or []):
            text_chunks: list[str] = []
            if isinstance(out.get("text"), list):
                text_chunks.extend(str(x) for x in out["text"])
            data = out.get("data") or {}
            if isinstance(data, dict):
                for mime_lines in data.values():
                    if isinstance(mime_lines, list):
                        text_chunks.extend(str(x) for x in mime_lines)
                    elif isinstance(mime_lines, str):
                        text_chunks.extend(mime_lines.splitlines())

            for chunk in text_chunks:
                for line in chunk.splitlines():
                    for rule_name, pattern in FORBIDDEN_SPECS:
                        if not pattern.search(line):
                            continue
                        if rule_name == "twelve_digit_number" and (
                            _BQ_BYTES_TD_RE.match(line) or _BQ_BYTES_ROW_RE.match(line)
                        ):
                            continue
                        hits.append(f"{path}:cell[{ci}].output[{oi}]: [{rule_name}]")
    return hits


def scan_plain_lines(path: Path, text: str) -> list[str]:
    hits: list[str] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        for rule_name, pattern in FORBIDDEN_SPECS:
            if pattern.search(line):
                hits.append(f"{path}:{line_no}: [{rule_name}]")
    return hits


files = collect_files()
all_hits: list[str] = []
for path in files:
    raw = path.read_bytes()
    if is_binary_bytes(raw):
        continue
    text = raw.decode("utf-8", errors="replace")
    rel = path.relative_to(repo_root) if path.is_relative_to(repo_root) else path

    if path.suffix == ".ipynb":
        all_hits.extend(scan_notebook(rel, text))
    elif path.name == "sample_benchmark_runs.jsonl":
        all_hits.extend(scan_jsonl_telemetry(rel, text))
    else:
        all_hits.extend(scan_plain_lines(rel, text))

if all_hits:
    print("FAIL: Forbidden internal or personal identifiers detected:", file=sys.stderr)
    for hit in all_hits:
        print(f"  - {hit}", file=sys.stderr)
    sys.exit(1)

print(f"PASS: Zero forbidden identifier matches across {len(files)} file(s).")
PY
