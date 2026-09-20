# Canh server SMC Monitor: cu 60 giay kiem tra mot lan, neu chet thi tu bat lai.
# Chay: bam dup vao watchdog.bat (giu cua so mo). Dung: dong cua so.
$ErrorActionPreference = 'SilentlyContinue'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$py   = Join-Path $root 'venv\Scripts\python.exe'
$log  = Join-Path $root 'watchdog.log'

function Write-Log($msg) {
    $line = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Add-Content -Path $log -Value $line -Encoding utf8
    Write-Host $line
}

function Test-Server {
    try { (Invoke-WebRequest -Uri 'http://localhost:8000/' -UseBasicParsing -TimeoutSec 8).StatusCode -eq 200 }
    catch { $false }
}

function Start-Server {
    Start-Process -FilePath $py -ArgumentList '-u', 'main.py' -WorkingDirectory $root -WindowStyle Minimized `
        -RedirectStandardOutput (Join-Path $root 'server.log') -RedirectStandardError (Join-Path $root 'server.err.log')
}

Write-Log "watchdog bat dau, kiem tra moi 60 giay"
if (-not (Test-Server)) {            # luc khoi dong: server chua chay thi bat ngay
    Write-Log "server chua chay - dang bat..."
    Start-Server
    Start-Sleep -Seconds 20
    if (Test-Server) { Write-Log "bat THANH CONG" } else { Write-Log "bat THAT BAI" }
} else {
    Write-Log "server dang chay binh thuong"
}
$fails = 0
while ($true) {
    if (Test-Server) {
        if ($fails -gt 0) { Write-Log "server da tro lai binh thuong" }
        $fails = 0
    } else {
        $fails++
        Write-Log "server khong phan hoi (lan $fails)"
        if ($fails -ge 2) {          # chac chan chet moi bat lai, tranh bat nham luc dang ban
            Write-Log "dang bat lai server..."
            Start-Server
            Start-Sleep -Seconds 20
            if (Test-Server) { Write-Log "bat lai THANH CONG" } else { Write-Log "bat lai THAT BAI, se thu lai" }
            $fails = 0
        }
    }
    Start-Sleep -Seconds 60
}
