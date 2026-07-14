# Smoke test: python web server -> true-imap-tunnel (server + client) -> parallel curl downloads.
#
# Generates client/server configs from env vars unless -ConfigDir (with client.yaml + server.yaml) is given:
#   TITS_IMAP_HOST  TITS_IMAP_PORT (993)  TITS_IMAP_USER  TITS_IMAP_PASS
#   TITS_IMAP_TLS (implicit)  TITS_ENC_PASS (smoke-test)
# Optional: -Bin <prebuilt tunnel binary>  -WebPort  -ListenPort
# When -ConfigDir is used, the server config's target must be 127.0.0.1:<WebPort> (this script's web server).

[CmdletBinding()]
param(
    [string]$ConfigDir,
    [string]$Bin,
    [int]$WebPort = 18080,
    [int]$ListenPort = 12222
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("tit-smoke-" + [guid]::NewGuid().ToString('N').Substring(0, 8))
$procs = @()
$exit = 1

function Fail($msg) { throw $msg }

try {
    New-Item -ItemType Directory -Force -Path $work, "$work\www" | Out-Null

    $files = [ordered]@{ 'f1.bin' = 16KB; 'f2.bin' = 64KB; 'f3.bin' = 128KB; 'f4.bin' = 256KB; 'f5.bin' = 32MB }
    $expected = @{}
    foreach ($name in $files.Keys) {
        $buf = New-Object byte[] ([int]$files[$name])
        (New-Object Random).NextBytes($buf)
        [System.IO.File]::WriteAllBytes("$work\www\$name", $buf)
        $expected[$name] = (Get-FileHash "$work\www\$name" -Algorithm SHA256).Hash
    }

    if (-not $Bin) {
        $Bin = Join-Path $work 'tit.exe'
        Push-Location $repoRoot
        & go build -o $Bin ./cmd/true-imap-tunnel
        Pop-Location
        if ($LASTEXITCODE -ne 0) { Fail 'go build failed' }
    }

    if ($ConfigDir) {
        $serverCfg = Join-Path $ConfigDir 'server.yaml'
        $clientCfg = Join-Path $ConfigDir 'client.yaml'
    }
    else {
        $h = $env:TITS_IMAP_HOST; $u = $env:TITS_IMAP_USER; $pw = $env:TITS_IMAP_PASS
        if (-not $h -or -not $u -or -not $pw) { Fail 'set TITS_IMAP_HOST / TITS_IMAP_USER / TITS_IMAP_PASS, or pass -ConfigDir' }
        $p = if ($env:TITS_IMAP_PORT) { $env:TITS_IMAP_PORT } else { '993' }
        $tls = if ($env:TITS_IMAP_TLS) { $env:TITS_IMAP_TLS } else { 'implicit' }
        $enc = if ($env:TITS_ENC_PASS) { $env:TITS_ENC_PASS } else { 'smoke-test' }
        $serverCfg = Join-Path $work 'server.yaml'
        $clientCfg = Join-Path $work 'client.yaml'
        @"
mode: server
target: "127.0.0.1:$WebPort"
log_level: info
accounts:
  - name: "primary"
    host: "$h`:$p"
    username: "$u"
    password: "$pw"
    tls: "$tls"
    folder_send: "TunnelS2C"
    folder_recv: "TunnelC2S"
reorder: true
encryption_passphrase: "$enc"
open_timeout_sec: 45
dial_timeout_sec: 10
poll_interval_ms: 2000
active_poll_interval_ms: 150
zero_rtt_open: true
async_data_send: true
"@ | Set-Content -Path $serverCfg -Encoding ascii
        @"
mode: client
listen: "127.0.0.1:$ListenPort"
log_level: info
accounts:
  - name: "primary"
    host: "$h`:$p"
    username: "$u"
    password: "$pw"
    tls: "$tls"
    folder_send: "TunnelC2S"
    folder_recv: "TunnelS2C"
reorder: true
encryption_passphrase: "$enc"
open_timeout_sec: 45
dial_timeout_sec: 10
poll_interval_ms: 2000
active_poll_interval_ms: 150
zero_rtt_open: true
async_data_send: true
"@ | Set-Content -Path $clientCfg -Encoding ascii
    }

    $procs += Start-Process -PassThru -WindowStyle Hidden -WorkingDirectory "$work\www" `
        -FilePath 'python' -ArgumentList '-m', 'http.server', "$WebPort", '--bind', '127.0.0.1'
    $procs += Start-Process -PassThru -WindowStyle Hidden -FilePath $Bin -ArgumentList '-config', $serverCfg `
        -RedirectStandardOutput "$work\server.log" -RedirectStandardError "$work\server.err"
    $procs += Start-Process -PassThru -WindowStyle Hidden -FilePath $Bin -ArgumentList '-config', $clientCfg `
        -RedirectStandardOutput "$work\client.log" -RedirectStandardError "$work\client.err"

    $base = "http://127.0.0.1:$ListenPort"
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 2
        $code = & curl.exe -s -o (Join-Path $work 'probe.out') -w '%{http_code}' --max-time 30 "$base/f1.bin" 2>$null
        if ($code -eq '200') { $ready = $true; break }
    }
    if (-not $ready) { Fail "tunnel not ready / probe download failed (see $work\client.log)" }

    $jobs = foreach ($name in $files.Keys) {
        Start-Job -ArgumentList $base, $name, $work -ScriptBlock {
            param($base, $name, $work)
            $out = Join-Path $work "dl_$name"
            $code = & curl.exe -s -o $out -w '%{http_code}' --max-time 600 "$base/$name"
            "$name|$code|$((Get-FileHash $out -Algorithm SHA256 -ErrorAction SilentlyContinue).Hash)"
        }
    }
    $results = $jobs | Wait-Job | Receive-Job
    $jobs | Remove-Job

    $ok = $true
    foreach ($r in ($results | Sort-Object)) {
        $name, $code, $hash = $r -split '\|'
        if ($code -eq '200' -and $hash -eq $expected[$name]) {
            Write-Host "  $name OK"
        }
        else {
            Write-Host "  $name FAIL (http=$code hash_match=$($hash -eq $expected[$name]))"
            $ok = $false
        }
    }
    if (-not $ok) { Fail 'one or more downloads did not match' }
    Write-Host "PASS: $($files.Count) files downloaded in parallel and verified"
    $exit = 0
}
catch {
    Write-Host "FAIL: $($_.Exception.Message)"
}
finally {
    foreach ($p in $procs) { if ($p -and -not $p.HasExited) { try { $p.Kill() } catch {} } }
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $work
}
exit $exit
