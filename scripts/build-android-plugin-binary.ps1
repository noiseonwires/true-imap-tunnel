param(
    [Parameter(Mandatory = $true)]
    [string]$Out,
    [string]$VersionName = "dev"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$hash = (git -C $repo rev-parse --short HEAD 2>$null)
if (-not $hash) { $hash = "unknown" }
$date = Get-Date -Format "yyyyMMdd-HHmmss"

$env:GOOS = "android"
$env:GOARCH = "arm64"
$env:CGO_ENABLED = "0"

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Out) | Out-Null
Set-Location $repo
go build -trimpath -ldflags "-s -w -X main.buildVersion=$VersionName -X main.buildDate=$date -X main.buildHash=$hash" -o $Out .\cmd\true-imap-tunnel
