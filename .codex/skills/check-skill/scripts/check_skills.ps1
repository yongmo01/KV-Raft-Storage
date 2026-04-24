param(
  [string[]]$Roots = @(".codex/skills"),
  [switch]$Strict
)

$NamePattern = '^[a-z0-9][a-z0-9-]{0,63}$'
$MojibakePatterns = @(
  [string][char]0xFFFD,
  [string][char]0x951B,
  [string][char]0x935B,
  [string][char]0x7487,
  [string][char]0x9239,
  [string][char]0x00C3,
  [string][char]0x00C2
)
$BannedDocNames = @("README.md", "INSTALLATION_GUIDE.md", "QUICK_REFERENCE.md", "CHANGELOG.md")
$ExpectedTopLevel = @("SKILL.md", "scripts", "references", "assets", "agents")
$LocalLinkPattern = '\[[^\]]+\]\(([^)]+)\)'

function Add-Finding {
  param(
    [System.Collections.Generic.List[object]]$Findings,
    [string]$Level,
    [string]$Skill,
    [string]$Message
  )
  $Findings.Add([pscustomobject]@{
    Level = $Level
    Skill = $Skill
    Message = $Message
  }) | Out-Null
}

function Get-SkillDirs {
  param([string]$Root)

  $resolved = Resolve-Path -LiteralPath $Root -ErrorAction SilentlyContinue
  if (-not $resolved) {
    return @()
  }

  $path = $resolved.Path
  if (Test-Path -LiteralPath (Join-Path $path "SKILL.md")) {
    return @((Get-Item -LiteralPath $path))
  }

  return @(Get-ChildItem -LiteralPath $path -Directory | Where-Object {
    Test-Path -LiteralPath (Join-Path $_.FullName "SKILL.md")
  })
}

function Parse-Frontmatter {
  param([string]$Text)

  $lines = $Text -split "`r?`n"
  if ($lines.Count -eq 0 -or $lines[0].Trim() -ne "---") {
    return @{
      Metadata = @{}
      Body = $Text
      Error = "missing YAML frontmatter delimiter"
    }
  }

  $endIndex = -1
  for ($i = 1; $i -lt $lines.Count; $i++) {
    if ($lines[$i].Trim() -eq "---") {
      $endIndex = $i
      break
    }
  }

  if ($endIndex -lt 0) {
    return @{
      Metadata = @{}
      Body = $Text
      Error = "unterminated YAML frontmatter"
    }
  }

  $metadata = @{}
  for ($i = 1; $i -lt $endIndex; $i++) {
    $line = $lines[$i].Trim()
    if (-not $line -or $line.StartsWith("#") -or -not $line.Contains(":")) {
      continue
    }
    $parts = $line.Split(":", 2)
    $metadata[$parts[0].Trim()] = $parts[1].Trim().Trim('"').Trim("'")
  }

  $body = ($lines[($endIndex + 1)..($lines.Count - 1)] -join "`n").Trim()
  $error = $null
  if ($metadata.Count -eq 0) {
    $error = "empty YAML frontmatter"
  }

  return @{
    Metadata = $metadata
    Body = $body
    Error = $error
  }
}

function Is-ExternalLink {
  param([string]$Target)
  $lower = $Target.ToLowerInvariant()
  return $lower.StartsWith("http://") `
    -or $lower.StartsWith("https://") `
    -or $lower.StartsWith("mailto:") `
    -or $lower.StartsWith("#") `
    -or $lower.StartsWith("app://") `
    -or $lower.StartsWith("plugin://")
}

function Check-Skill {
  param([System.IO.DirectoryInfo]$SkillDir)

  $findings = [System.Collections.Generic.List[object]]::new()
  $skillFile = Join-Path $SkillDir.FullName "SKILL.md"

  if (-not (Test-Path -LiteralPath $skillFile)) {
    Add-Finding $findings "ERROR" $SkillDir.FullName "missing SKILL.md"
    return $findings
  }

  try {
    $text = Get-Content -LiteralPath $skillFile -Raw -Encoding UTF8
  } catch {
    Add-Finding $findings "ERROR" $SkillDir.FullName "SKILL.md is not valid UTF-8"
    return $findings
  }

  $parsed = Parse-Frontmatter $text
  $metadata = $parsed.Metadata
  $body = $parsed.Body
  if ($parsed.Error) {
    Add-Finding $findings "ERROR" $SkillDir.FullName $parsed.Error
  }

  $name = ""
  if ($metadata.ContainsKey("name")) { $name = $metadata["name"] }
  $description = ""
  if ($metadata.ContainsKey("description")) { $description = $metadata["description"] }

  if (-not $name) {
    Add-Finding $findings "ERROR" $SkillDir.FullName "frontmatter missing required field: name"
  } elseif ($name -notmatch $NamePattern) {
    Add-Finding $findings "ERROR" $SkillDir.FullName "name must use lowercase letters, digits, and hyphens: '$name'"
  } elseif ($SkillDir.Name -ne $name) {
    Add-Finding $findings "ERROR" $SkillDir.FullName "folder name '$($SkillDir.Name)' does not match skill name '$name'"
  }

  if (-not $description) {
    Add-Finding $findings "ERROR" $SkillDir.FullName "frontmatter missing required field: description"
  } elseif ($description.Length -lt 40) {
    Add-Finding $findings "WARN" $SkillDir.FullName "description may be too short to trigger reliably"
  } elseif ($description.ToLowerInvariant() -notmatch "when") {
    Add-Finding $findings "WARN" $SkillDir.FullName "description should clearly state when to use the skill"
  }

  if (-not $body) {
    Add-Finding $findings "ERROR" $SkillDir.FullName "SKILL.md has no instruction body after frontmatter"
  }

  $lineCount = ($text -split "`r?`n").Count
  if ($lineCount -gt 500) {
    Add-Finding $findings "WARN" $SkillDir.FullName "SKILL.md is long (${lineCount} lines); consider references/"
  }

  foreach ($pattern in $MojibakePatterns) {
    if ($text.Contains($pattern)) {
      Add-Finding $findings "WARN" $SkillDir.FullName "possible mojibake pattern found in SKILL.md: '$pattern'"
      break
    }
  }

  $linkedPaths = [System.Collections.Generic.HashSet[string]]::new()
  foreach ($match in [regex]::Matches($text, $LocalLinkPattern)) {
    $target = $match.Groups[1].Value.Split("#", 2)[0].Replace("%20", " ").Trim()
    if (-not $target -or (Is-ExternalLink $target)) {
      continue
    }
    if ($target.StartsWith("/") -or $target -match "^[A-Za-z]:[\\/]") {
      continue
    }
    $resolved = [System.IO.Path]::GetFullPath((Join-Path $SkillDir.FullName $target))
    [void]$linkedPaths.Add($resolved)
    if (-not (Test-Path -LiteralPath $resolved)) {
      Add-Finding $findings "ERROR" $SkillDir.FullName "broken local link in SKILL.md: $target"
    }
  }

  $referencesDir = Join-Path $SkillDir.FullName "references"
  if (Test-Path -LiteralPath $referencesDir) {
    Get-ChildItem -LiteralPath $referencesDir -Recurse -File | ForEach-Object {
      $full = [System.IO.Path]::GetFullPath($_.FullName)
      if (-not $linkedPaths.Contains($full)) {
        Add-Finding $findings "WARN" $SkillDir.FullName "reference file is not linked from SKILL.md: $($_.FullName.Substring($SkillDir.FullName.Length + 1))"
      }
    }
  }

  Get-ChildItem -LiteralPath $SkillDir.FullName -Recurse -File | ForEach-Object {
    if ($BannedDocNames -contains $_.Name) {
      Add-Finding $findings "WARN" $SkillDir.FullName "avoid auxiliary document in skill folder: $($_.FullName.Substring($SkillDir.FullName.Length + 1))"
    }
    try {
      $content = Get-Content -LiteralPath $_.FullName -Raw -Encoding UTF8
      foreach ($pattern in $MojibakePatterns) {
        if ($content.Contains($pattern)) {
          Add-Finding $findings "WARN" $SkillDir.FullName "possible mojibake pattern found in $($_.FullName.Substring($SkillDir.FullName.Length + 1)): '$pattern'"
          break
        }
      }
    } catch {
      Add-Finding $findings "WARN" $SkillDir.FullName "non UTF-8 text file or binary file detected: $($_.FullName.Substring($SkillDir.FullName.Length + 1))"
    }
  }

  Get-ChildItem -LiteralPath $SkillDir.FullName | ForEach-Object {
    if ($ExpectedTopLevel -notcontains $_.Name) {
      Add-Finding $findings "WARN" $SkillDir.FullName "unexpected top-level item: $($_.Name)"
    }
  }

  $agentsDir = Join-Path $SkillDir.FullName "agents"
  if ((Test-Path -LiteralPath $agentsDir) -and -not (Test-Path -LiteralPath (Join-Path $agentsDir "openai.yaml"))) {
    Add-Finding $findings "WARN" $SkillDir.FullName "agents/ exists but agents/openai.yaml is missing"
  }

  return $findings
}

$skillDirs = @()
foreach ($root in $Roots) {
  $skillDirs += Get-SkillDirs $root
}

if ($skillDirs.Count -eq 0) {
  Write-Error "no skill directories found"
  exit 2
}

$allFindings = @()
foreach ($skillDir in $skillDirs) {
  $findings = @(Check-Skill $skillDir)
  $allFindings += $findings
  if ($findings.Count -eq 0) {
    Write-Host "OK: $($skillDir.FullName)"
  } else {
    Write-Host ""
    Write-Host $skillDir.FullName
    foreach ($finding in $findings) {
      Write-Host "  $($finding.Level): $($finding.Message)"
    }
  }
}

$errors = @($allFindings | Where-Object { $_.Level -eq "ERROR" }).Count
$warnings = @($allFindings | Where-Object { $_.Level -eq "WARN" }).Count
Write-Host ""
Write-Host "Checked $($skillDirs.Count) skill(s): $errors error(s), $warnings warning(s)"

if ($errors -gt 0 -or ($Strict -and $warnings -gt 0)) {
  exit 1
}

exit 0
