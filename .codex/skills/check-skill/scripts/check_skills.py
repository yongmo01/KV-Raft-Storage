#!/usr/bin/env python3
"""Validate Codex skill folders.

This checker intentionally uses only the Python standard library so it can run
in lightweight repositories without extra setup.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
LOCAL_LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
MOJIBAKE_PATTERNS = (
    chr(0xFFFD),
    chr(0x951B),
    chr(0x935B),
    chr(0x7487),
    chr(0x9239),
    chr(0x00C3),
    chr(0x00C2),
)
BANNED_DOC_NAMES = {
    "README.md",
    "INSTALLATION_GUIDE.md",
    "QUICK_REFERENCE.md",
    "CHANGELOG.md",
}
EXPECTED_TOP_LEVEL = {"SKILL.md", "scripts", "references", "assets", "agents"}


@dataclass
class Finding:
    level: str
    skill: Path
    message: str


def parse_frontmatter(text: str) -> tuple[dict[str, str], str, str | None]:
    if not text.startswith("---\n") and not text.startswith("---\r\n"):
        return {}, text, "missing YAML frontmatter delimiter"

    lines = text.splitlines()
    end_index = None
    for idx in range(1, len(lines)):
        if lines[idx].strip() == "---":
            end_index = idx
            break

    if end_index is None:
        return {}, text, "unterminated YAML frontmatter"

    metadata: dict[str, str] = {}
    for raw in lines[1:end_index]:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip().strip("\"'")

    body = "\n".join(lines[end_index + 1 :]).strip()
    frontmatter = "\n".join(lines[1:end_index])
    return metadata, body, None if metadata else "empty YAML frontmatter"


def is_external_link(target: str) -> bool:
    lowered = target.lower()
    return (
        lowered.startswith("http://")
        or lowered.startswith("https://")
        or lowered.startswith("mailto:")
        or lowered.startswith("#")
        or lowered.startswith("app://")
        or lowered.startswith("plugin://")
    )


def normalize_link_target(target: str) -> str:
    target = target.split("#", 1)[0]
    target = target.replace("%20", " ")
    return target.strip()


def iter_skill_dirs(root: Path) -> Iterable[Path]:
    if (root / "SKILL.md").is_file():
        yield root
        return
    if not root.exists():
        return
    for child in sorted(root.iterdir()):
        if child.is_dir() and (child / "SKILL.md").is_file():
            yield child


def check_skill(skill_dir: Path) -> list[Finding]:
    findings: list[Finding] = []
    skill_file = skill_dir / "SKILL.md"

    if not skill_file.is_file():
        findings.append(Finding("ERROR", skill_dir, "missing SKILL.md"))
        return findings

    try:
        text = skill_file.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        findings.append(Finding("ERROR", skill_dir, "SKILL.md is not valid UTF-8"))
        return findings

    metadata, body, fm_error = parse_frontmatter(text)
    if fm_error:
        findings.append(Finding("ERROR", skill_dir, fm_error))

    name = metadata.get("name", "")
    description = metadata.get("description", "")

    if not name:
        findings.append(Finding("ERROR", skill_dir, "frontmatter missing required field: name"))
    elif not NAME_RE.match(name):
        findings.append(
            Finding("ERROR", skill_dir, f"name must use lowercase letters, digits, and hyphens: {name!r}")
        )
    elif skill_dir.name != name:
        findings.append(
            Finding("ERROR", skill_dir, f"folder name {skill_dir.name!r} does not match skill name {name!r}")
        )

    if not description:
        findings.append(Finding("ERROR", skill_dir, "frontmatter missing required field: description"))
    elif len(description) < 40:
        findings.append(Finding("WARN", skill_dir, "description may be too short to trigger reliably"))
    elif "use when" not in description.lower() and "when" not in description.lower():
        findings.append(Finding("WARN", skill_dir, "description should clearly state when to use the skill"))

    if not body:
        findings.append(Finding("ERROR", skill_dir, "SKILL.md has no instruction body after frontmatter"))

    line_count = len(text.splitlines())
    if line_count > 500:
        findings.append(Finding("WARN", skill_dir, f"SKILL.md is long ({line_count} lines); consider references/"))

    for pattern in MOJIBAKE_PATTERNS:
        if pattern in text:
            findings.append(Finding("WARN", skill_dir, f"possible mojibake pattern found in SKILL.md: {pattern!r}"))
            break

    linked_paths: set[Path] = set()
    for match in LOCAL_LINK_RE.finditer(text):
        target = normalize_link_target(match.group(1))
        if not target or is_external_link(target):
            continue
        if target.startswith("/"):
            continue
        if re.match(r"^[A-Za-z]:[\\/]", target):
            continue
        resolved = (skill_dir / target).resolve()
        linked_paths.add(resolved)
        if not resolved.exists():
            findings.append(Finding("ERROR", skill_dir, f"broken local link in SKILL.md: {target}"))

    references_dir = skill_dir / "references"
    if references_dir.is_dir():
        for ref in sorted(references_dir.rglob("*")):
            if ref.is_file() and ref.resolve() not in linked_paths:
                findings.append(Finding("WARN", skill_dir, f"reference file is not linked from SKILL.md: {ref.relative_to(skill_dir)}"))

    for path in skill_dir.rglob("*"):
        if path.is_file() and path.name in BANNED_DOC_NAMES:
            findings.append(Finding("WARN", skill_dir, f"avoid auxiliary document in skill folder: {path.relative_to(skill_dir)}"))
        if path.is_file():
            try:
                content = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                findings.append(Finding("WARN", skill_dir, f"non UTF-8 text file or binary file detected: {path.relative_to(skill_dir)}"))
                continue
            for pattern in MOJIBAKE_PATTERNS:
                if pattern in content:
                    findings.append(Finding("WARN", skill_dir, f"possible mojibake pattern found in {path.relative_to(skill_dir)}: {pattern!r}"))
                    break

    for child in skill_dir.iterdir():
        if child.name not in EXPECTED_TOP_LEVEL:
            findings.append(Finding("WARN", skill_dir, f"unexpected top-level item: {child.name}"))

    agents_dir = skill_dir / "agents"
    if agents_dir.is_dir() and not (agents_dir / "openai.yaml").is_file():
        findings.append(Finding("WARN", skill_dir, "agents/ exists but agents/openai.yaml is missing"))

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Codex skill folders.")
    parser.add_argument(
        "roots",
        nargs="*",
        default=[".codex/skills"],
        help="Skill root directories or individual skill directories. Default: .codex/skills",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as failures.",
    )
    args = parser.parse_args()

    skill_dirs: list[Path] = []
    for raw_root in args.roots:
        root = Path(os.path.expanduser(raw_root)).resolve()
        skill_dirs.extend(iter_skill_dirs(root))

    if not skill_dirs:
        print("ERROR: no skill directories found", file=sys.stderr)
        return 2

    all_findings: list[Finding] = []
    for skill_dir in skill_dirs:
        findings = check_skill(skill_dir)
        all_findings.extend(findings)
        if findings:
            print(f"\n{skill_dir}")
            for finding in findings:
                print(f"  {finding.level}: {finding.message}")
        else:
            print(f"OK: {skill_dir}")

    errors = sum(1 for finding in all_findings if finding.level == "ERROR")
    warnings = sum(1 for finding in all_findings if finding.level == "WARN")
    checked = len(skill_dirs)
    print(f"\nChecked {checked} skill(s): {errors} error(s), {warnings} warning(s)")

    if errors or (args.strict and warnings):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
