---
name: check-skill
description: Use when the user wants to audit, validate, or quality-check Codex skill folders for common structural, metadata, reference-link, naming, and maintainability problems.
---

# Check Skill

Use this skill to check other skills before relying on them.

## Quick Run

From the repository root:

```bash
python .codex/skills/check-skill/scripts/check_skills.py .codex/skills
```

On Windows without Python:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .codex\skills\check-skill\scripts\check_skills.ps1 .codex\skills
```

To scan multiple roots:

```bash
python .codex/skills/check-skill/scripts/check_skills.py .codex/skills ~/.codex/skills
```

## What The Checker Validates

The script checks:

- `SKILL.md` exists in each skill folder.
- Frontmatter exists and has `name` and `description`.
- `name` uses lowercase letters, digits, and hyphens.
- Skill folder name matches frontmatter `name`.
- Description is long enough to be a useful trigger.
- Body content exists after frontmatter.
- `SKILL.md` is not excessively long.
- Markdown links to local files resolve.
- Files under `references/` are linked from `SKILL.md`.
- Avoided auxiliary documents such as `README.md`, `CHANGELOG.md`, and `QUICK_REFERENCE.md`.
- Common mojibake patterns are absent.

## Result Levels

- `ERROR`: fix before using the skill.
- `WARN`: usually fix, but may be acceptable if intentional.
- `OK`: no detected structural issues.

## Guidance

If the checker reports errors:

1. Fix `SKILL.md` frontmatter first.
2. Fix broken local links.
3. Remove or move unnecessary auxiliary documents.
4. Re-run the checker.

Do not treat this as a semantic proof that a skill is good. It catches structural and maintenance issues; the actual workflow still needs human review.
