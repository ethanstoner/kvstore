# verify.ps1 — end-to-end verification that kvstore actually works.
#
# Runs three checks beyond the unit-test suite:
#   1. Live RESP smoke test against a real TCP server (17 commands)
#   2. Crash recovery: write 5,000 keys, kill -9 the server, restart, verify
#   3. JMH performance benchmarks (optional, slow — pass -SkipBench to skip)
#
# Run from the project root:
#   pwsh -File verify.ps1
#   pwsh -File verify.ps1 -SkipBench

param([switch]$SkipBench)

# Prefers whatever `java` and `mvn` are already on PATH (JDK 21+ and Maven, per the
# README), falling back to the usual Windows install locations so a machine that has
# them installed but not on PATH still runs.
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    $jdk = Get-ChildItem "$env:ProgramFiles\Microsoft\jdk-21*" -Directory -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending | Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName; $env:PATH = "$($jdk.FullName)\bin;$env:PATH" }
}
if (-not (Get-Command mvn -ErrorAction SilentlyContinue)) {
    $m2 = Get-ChildItem "$env:USERPROFILE\tools\maven\apache-maven-*\bin" -Directory -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending | Select-Object -First 1
    if ($m2) { $env:PATH = "$($m2.FullName);$env:PATH" }
}
foreach ($tool in 'java', 'mvn') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        Write-Error "$tool was not found. Install JDK 21+ and Maven, or add them to PATH."
        exit 1
    }
}

Set-Location $PSScriptRoot

# Stops only the servers this script started. Matching on the jar name matters: a
# bare `Get-Process java | Stop-Process` kills every JVM on the machine, including
# unrelated work that happens to be running.
function Stop-KvServers {
    Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*kvstore*' } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

function MkResp { param([string[]]$Parts)
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.Append("*$($Parts.Count)`r`n")
    foreach ($a in $Parts) {
        $bytes = [System.Text.Encoding]::UTF8.GetByteCount($a)
        [void]$sb.Append("`$$bytes`r`n$a`r`n")
    }
    return $sb.ToString()
}

function Send-Multi { param([string]$Address, [int]$Port, [string[]]$Commands)
    $client = New-Object System.Net.Sockets.TcpClient($Address, $Port)
    $stream = $client.GetStream()
    $replies = @()
    foreach ($c in $Commands) {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($c)
        $stream.Write($bytes, 0, $bytes.Length); $stream.Flush()
        Start-Sleep -Milliseconds 100
        $buf = New-Object byte[] 8192
        try { $n = $stream.Read($buf, 0, $buf.Length); $replies += [System.Text.Encoding]::UTF8.GetString($buf, 0, $n) }
        catch { $replies += "[error]" }
    }
    $client.Close()
    return $replies
}

function Fmt { param([string]$s) return $s.Replace("`r","\r").Replace("`n","\n") }

# ----------------------------------------------------------------------------
Write-Output "=== Build ==="
if (-not (Test-Path target/kvstore-0.1.0.jar)) {
    mvn package -DskipTests -q
}
if (-not (Test-Path target/kvstore-0.1.0.jar)) {
    Write-Error "Build failed"; exit 1
}
Write-Output "Built target/kvstore-0.1.0.jar"

# ----------------------------------------------------------------------------
Write-Output ""
Write-Output "=== 1. Live RESP smoke test ==="

Stop-KvServers
Start-Sleep -Seconds 1
if (Test-Path verify-data) { Remove-Item -Recurse -Force verify-data -ErrorAction SilentlyContinue }

$srv = Start-Process -FilePath "java" -ArgumentList "-jar","target/kvstore-0.1.0.jar","serve","--port","6392","--data","verify-data" -NoNewWindow -RedirectStandardOutput "verify-srv.log" -RedirectStandardError "verify-srv.err.log" -PassThru
Start-Sleep -Seconds 3

$tests = @(
    @{ Name="PING";              Cmd=(MkResp @("PING"));                                Expect="+PONG`r`n" },
    @{ Name="SET hello";         Cmd=(MkResp @("SET","hello","world"));                 Expect="+OK`r`n" },
    @{ Name="GET hello";         Cmd=(MkResp @("GET","hello"));                         Expect="`$5`r`nworld`r`n" },
    @{ Name="INCR counter";      Cmd=(MkResp @("INCR","counter"));                      Expect=":1`r`n" },
    @{ Name="INCRBY counter 9";  Cmd=(MkResp @("INCRBY","counter","9"));                Expect=":10`r`n" },
    @{ Name="DECR counter";      Cmd=(MkResp @("DECR","counter"));                      Expect=":9`r`n" },
    @{ Name="SET ttl EX 60";     Cmd=(MkResp @("SET","ttlkey","tmp","EX","60"));        Expect="+OK`r`n" },
    @{ Name="TTL ttlkey";        Cmd=(MkResp @("TTL","ttlkey"));                        ExpectPattern="^:[1-9]\d*`r`n$" },
    @{ Name="EXISTS hello miss"; Cmd=(MkResp @("EXISTS","hello","missing"));            Expect=":1`r`n" },
    @{ Name="MGET 3 keys";       Cmd=(MkResp @("MGET","hello","counter","missing"));    ExpectPattern="^\*3`r`n" },
    @{ Name="DBSIZE";            Cmd=(MkResp @("DBSIZE"));                              ExpectPattern="^:\d+`r`n$" },
    @{ Name="SCAN a z";          Cmd=(MkResp @("SCAN","a","z"));                        ExpectPattern="^\*\d+`r`n" },
    @{ Name="BGSAVE";            Cmd=(MkResp @("BGSAVE"));                              Expect="+Background saving started`r`n" },
    @{ Name="LASTSAVE";          Cmd=(MkResp @("LASTSAVE"));                            ExpectPattern="^:\d+`r`n$" },
    @{ Name="DEL hello";         Cmd=(MkResp @("DEL","hello"));                         Expect=":1`r`n" },
    @{ Name="GET hello (gone)";  Cmd=(MkResp @("GET","hello"));                         Expect="`$-1`r`n" },
    @{ Name="Unknown cmd";       Cmd=(MkResp @("FROBNICATE"));                          ExpectPattern="^-ERR unknown" }
)

$replies = Send-Multi "127.0.0.1" 6392 ($tests | ForEach-Object { $_.Cmd })
$pass = 0; $fail = 0
for ($i = 0; $i -lt $tests.Count; $i++) {
    $t = $tests[$i]; $r = $replies[$i]
    $ok = $false
    if ($t.Expect) { $ok = ($r -eq $t.Expect) }
    elseif ($t.ExpectPattern) { $ok = ($r -match $t.ExpectPattern) }
    if ($ok) { $pass++ } else { $fail++ }
    $status = if ($ok) { "PASS" } else { "FAIL" }
    Write-Output ("[$status] {0,-22} -> {1}" -f $t.Name, (Fmt $r))
}
Write-Output "Smoke test: $pass passed, $fail failed"

Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

# ----------------------------------------------------------------------------
Write-Output ""
Write-Output "=== 2. Crash recovery test ==="

if (Test-Path verify-data) { Remove-Item -Recurse -Force verify-data -ErrorAction SilentlyContinue }

$srv = Start-Process -FilePath "java" -ArgumentList "-jar","target/kvstore-0.1.0.jar","serve","--port","6393","--data","verify-data" -NoNewWindow -RedirectStandardOutput "verify-srv.log" -RedirectStandardError "verify-srv.err.log" -PassThru
Start-Sleep -Seconds 3
Write-Output "Server PID: $($srv.Id) — writing 5,000 keys..."

$cmds = @()
for ($i = 0; $i -lt 5000; $i++) { $cmds += MkResp @("SET", "key-$($i.ToString('D5'))", "value-$i") }
$null = Send-Multi "127.0.0.1" 6393 $cmds

$pre = Send-Multi "127.0.0.1" 6393 @((MkResp @("DBSIZE")))
Write-Output ("Pre-crash DBSIZE: " + (Fmt $pre[0]))

Write-Output "Killing server with -9..."
Stop-Process -Id $srv.Id -Force
Start-Sleep -Seconds 2

Write-Output "Restarting..."
$srv2 = Start-Process -FilePath "java" -ArgumentList "-jar","target/kvstore-0.1.0.jar","serve","--port","6394","--data","verify-data" -NoNewWindow -RedirectStandardOutput "verify-srv.log" -RedirectStandardError "verify-srv.err.log" -PassThru
Start-Sleep -Seconds 4

$verify = Send-Multi "127.0.0.1" 6394 @(
    (MkResp @("GET","key-00000")),
    (MkResp @("GET","key-02500")),
    (MkResp @("GET","key-04999")),
    (MkResp @("DBSIZE"))
)
$survivors = 0
if ($verify[0] -match "value-0`r`n$")    { Write-Output "[PASS] key-00000 survived"; $survivors++ } else { Write-Output "[FAIL] key-00000 -> $(Fmt $verify[0])" }
if ($verify[1] -match "value-2500`r`n$") { Write-Output "[PASS] key-02500 survived"; $survivors++ } else { Write-Output "[FAIL] key-02500 -> $(Fmt $verify[1])" }
if ($verify[2] -match "value-4999`r`n$") { Write-Output "[PASS] key-04999 survived"; $survivors++ } else { Write-Output "[FAIL] key-04999 -> $(Fmt $verify[2])" }
Write-Output ("Post-crash DBSIZE: " + (Fmt $verify[3]))
Write-Output "Crash recovery: $survivors / 3 spot-checked keys survived"

Stop-Process -Id $srv2.Id -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

# ----------------------------------------------------------------------------
if (-not $SkipBench) {
    Write-Output ""
    Write-Output "=== 3. JMH benchmarks (~2 min) ==="
    if (-not (Test-Path target/kvstore-0.1.0-benchmarks.jar)) {
        Write-Output "Building benchmarks jar..."
        mvn package -DskipTests -q
    }
    java -jar target/kvstore-0.1.0-benchmarks.jar -wi 1 -i 2 -f 1 -t 1 2>&1 | Select-Object -Last 12
}

# ----------------------------------------------------------------------------
# Cleanup
Stop-KvServers
Start-Sleep -Seconds 1
if (Test-Path verify-data) { Remove-Item -Recurse -Force verify-data -ErrorAction SilentlyContinue }
Remove-Item verify-srv.log,verify-srv.err.log -ErrorAction SilentlyContinue

Write-Output ""
Write-Output "=== Done ==="
