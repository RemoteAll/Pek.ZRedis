#requires -Version 5.1
<#
.SYNOPSIS
    Pek.ZRedis 跨语言严格对拍：Zig ↔ C#/Rust（同一实例、同一键、同一字节）。

.DESCRIPTION
    三者共用同一 Redis 实例与同一套键/编码，所以「对齐」的判据是**字节**。本脚本用
    **独立实现（redis-cli）** 做外部口径，双向验证：

    1. Zig 写 → redis-cli 读：逐行断言键名、值字节、TTL、score 文本、队列/Stream 布局；
    2. redis-cli 写（`--pipe` 灌精确 RESP，规避 PS 5.1 argv 编码问题）→ Zig 读：`interop verify`；
    3. Zig 自洽：`interop write` → `interop verify`（新进程）。

    互不干扰：自建**临时 redis-server 实例**（随机端口 + 临时目录 + 关闭持久化），
    绝不碰用户已运行的实例。

.NOTES
    - 缺 redis-server / redis-cli 时打印 `SKIP: ...` 并退出 0（显式跳过，不算通过）；
    - 可用环境变量或参数指定二进制：`PEK_ZREDIS_REDIS_SERVER` / `PEK_ZREDIS_REDIS_CLI`；
    - PS 5.1：外部命令一律 Start-Process + 重定向到文件再读字节，避免 stdout 被 ANSI 解码破坏二进制。
#>
[CmdletBinding()]
param(
    [string]$RedisServer,
    [string]$RedisCli,
    [string]$Prefix = 'pezredis:interop:',
    [int]$Port = 0,
    [switch]$NoBuild,
    [switch]$KeepServer
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$script:seq = 0
$script:failures = 0
$script:checks = 0

function Write-Line {
    param([string]$Text)
    Write-Host $Text
}

function Resolve-Tool {
    param([string]$Explicit, [string]$EnvName, [string]$FileName, [string[]]$Candidates)
    if ($Explicit -and (Test-Path -LiteralPath $Explicit)) { return (Resolve-Path -LiteralPath $Explicit).Path }
    $fromEnv = [Environment]::GetEnvironmentVariable($EnvName)
    if ($fromEnv -and (Test-Path -LiteralPath $fromEnv)) { return (Resolve-Path -LiteralPath $fromEnv).Path }
    foreach ($c in $Candidates) {
        if ($c -and (Test-Path -LiteralPath $c)) { return (Resolve-Path -LiteralPath $c).Path }
    }
    $cmd = Get-Command $FileName -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}

function Assert-Ok {
    param([string]$Name, [bool]$Ok, [string]$Detail)
    $script:checks++
    if ($Ok) {
        Write-Line "CHECK=$Name OK"
        return
    }
    $script:failures++
    Write-Line "CHECK=$Name FAIL $Detail"
}

function ConvertTo-Hex {
    param([byte[]]$Bytes)
    if ($null -eq $Bytes) { return '<nil>' }
    return (($Bytes | ForEach-Object { $_.ToString('X2') }) -join ' ')
}

function ConvertTo-Bytes {
    param([string]$Text)
    return [System.Text.Encoding]::UTF8.GetBytes($Text)
}

function Test-BytesEqual {
    param([byte[]]$Actual, [byte[]]$Expected)
    if ($null -eq $Actual -or $null -eq $Expected) { return $false }
    if ($Actual.Length -ne $Expected.Length) { return $false }
    for ($i = 0; $i -lt $Actual.Length; $i++) {
        if ($Actual[$i] -ne $Expected[$i]) { return $false }
    }
    return $true
}

# ------------------------------------------------------------------ 进程封装

function Invoke-Zredis {
    param([string]$Label, [string[]]$Arguments)
    $out = Join-Path $script:tmp "$Label.out.txt"
    $err = Join-Path $script:tmp "$Label.err.txt"
    $proc = Start-Process -FilePath $script:zredis -ArgumentList $Arguments `
        -RedirectStandardOutput $out -RedirectStandardError $err -PassThru -Wait -NoNewWindow
    $stdout = if (Test-Path -LiteralPath $out) { Get-Content -LiteralPath $out -Raw -Encoding UTF8 } else { '' }
    $stderr = if (Test-Path -LiteralPath $err) { Get-Content -LiteralPath $err -Raw -Encoding UTF8 } else { '' }
    return [pscustomobject]@{
        ExitCode = $proc.ExitCode
        Text     = ("$stdout`n$stderr").Trim()
    }
}

function Invoke-RedisCliRaw {
    param([string[]]$Arguments)
    $script:seq++
    $out = Join-Path $script:tmp ("cli-{0:d3}-{1}.bin" -f $script:seq, ($Arguments -join '_' -replace '[^A-Za-z0-9_-]', ''))
    $err = "$out.err"
    $proc = Start-Process -FilePath $script:redisCli `
        -ArgumentList (@('-p', "$script:port", '--raw') + $Arguments) `
        -RedirectStandardOutput $out -RedirectStandardError $err -PassThru -Wait -NoNewWindow
    if ($proc.ExitCode -ne 0) {
        $msg = if (Test-Path -LiteralPath $err) { Get-Content -LiteralPath $err -Raw -Encoding UTF8 } else { '' }
        throw "redis-cli $($Arguments -join ' ') 失败（exit $($proc.ExitCode)）：$msg"
    }
    $bytes = [System.IO.File]::ReadAllBytes($out)
    # redis-cli --raw 会给每条回复追加换行；值本身已按 RESP 长度截取，安全地剥掉尾部 CR/LF
    $len = $bytes.Length
    while ($len -gt 0 -and ($bytes[$len - 1] -eq 10 -or $bytes[$len - 1] -eq 13)) { $len-- }
    if ($len -eq $bytes.Length) { return $bytes }
    return $bytes[0..($len - 1)]
}

function Invoke-RedisPipe {
    param([string]$PipeFile)
    $out = Join-Path $script:tmp 'pipe.out.txt'
    $err = Join-Path $script:tmp 'pipe.err.txt'
    $proc = Start-Process -FilePath $script:redisCli `
        -ArgumentList @('-p', "$script:port", '--pipe') `
        -RedirectStandardInput $PipeFile -RedirectStandardOutput $out -RedirectStandardError $err `
        -PassThru -Wait -NoNewWindow
    $text = (Get-Content -LiteralPath $out -Raw -Encoding UTF8) + (Get-Content -LiteralPath $err -Raw -Encoding UTF8)
    if ($proc.ExitCode -ne 0 -or $text -notmatch 'errors:\s*0') {
        throw "redis-cli --pipe 失败：$($text.Trim())"
    }
}

# ------------------------------------------------------------------ RESP 组包

function Add-Bulk {
    param([System.Collections.Generic.List[byte]]$Buffer, [byte[]]$Payload)
    $head = [byte[]](ConvertTo-Bytes ("`${0}`r`n" -f $Payload.Length))
    $Buffer.AddRange($head)
    $Buffer.AddRange([byte[]]$Payload)
    $Buffer.AddRange([byte[]]@(13, 10))
}

function New-PipeFile {
    param([string]$Path, [object[]]$Commands)
    $buf = New-Object 'System.Collections.Generic.List[byte]'
    foreach ($cmd in $Commands) {
        $head = [byte[]](ConvertTo-Bytes ("*{0}`r`n" -f $cmd.Count))
        $buf.AddRange($head)
        foreach ($arg in $cmd) {
            if ($arg -is [byte[]]) { Add-Bulk -Buffer $buf -Payload $arg }
            else { Add-Bulk -Buffer $buf -Payload ([byte[]](ConvertTo-Bytes ([string]$arg))) }
        }
    }
    [System.IO.File]::WriteAllBytes($Path, $buf.ToArray())
}

# ------------------------------------------------------------------ 规范数据

$binValue = [byte[]]@(0x00, 0x01, 0x0d, 0x0a, 0x22, 0xff)
$spec = @{
    plain    = 'hello-Zig'
    utf8     = '中文-对齐-🚀'
    ttl      = 'ttl-value'
    ttlSec   = 120
    counter  = '3'
    bool     = 'True'
    hashF1   = '1'
    hashF2   = '中文'
    list     = @('a', 'b', 'c')
    set      = @('x', 'y')
    z1       = '1.5'
    z2       = '-2.25'
    queue    = 'q1'
    delay    = 'd1'
    delaySec = 60
    reliable = 'r1'
    streamF  = 'f1'
    streamV  = 'v1'
    lockMs   = 30000
    lockTok  = '0123456789abcdef0123456789abcdef'
}

function K {
    param([string]$Suffix)
    return "$Prefix$Suffix"
}

# ------------------------------------------------------------------ 主流程

$server = Resolve-Tool -Explicit $RedisServer -EnvName 'PEK_ZREDIS_REDIS_SERVER' -FileName 'redis-server.exe' `
    -Candidates @('D:\Redis\redis-server.exe', 'C:\Redis\redis-server.exe')
$cli = Resolve-Tool -Explicit $RedisCli -EnvName 'PEK_ZREDIS_REDIS_CLI' -FileName 'redis-cli.exe' `
    -Candidates @('D:\Redis\redis-cli.exe', 'C:\Redis\redis-cli.exe')
if (-not $server -or -not $cli) {
    Write-Line "SKIP: 未找到 redis-server/redis-cli（可用 -RedisServer/-RedisCli 或 PEK_ZREDIS_REDIS_SERVER/PEK_ZREDIS_REDIS_CLI 指定）"
    exit 0
}

$zredis = Join-Path $root 'zig-out\bin\zredis.exe'
if (-not $NoBuild) {
    Push-Location $root
    try {
        & zig build
        if ($LASTEXITCODE -ne 0) { throw "zig build 失败（exit $LASTEXITCODE）" }
    } finally {
        Pop-Location
    }
}
if (-not (Test-Path -LiteralPath $zredis)) {
    Write-Line "SKIP: 缺少 CLI 产物 $zredis（先 zig build，或去掉 -NoBuild）"
    exit 0
}

$script:tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("pezredis-interop-" + [System.Guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Force -Path $script:tmp | Out-Null
$script:zredis = $zredis
$script:redisCli = $cli
if ($Port -le 0) {
    $probe = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
    $probe.Start()
    $script:port = ([System.Net.IPEndPoint]$probe.LocalEndpoint).Port
    $probe.Stop()
} else {
    $script:port = $Port
}

$conf = Join-Path $script:tmp 'redis.conf'
$confText = @(
    "port $script:port",
    'bind 127.0.0.1',
    'protected-mode no',
    'save ""',
    'appendonly no',
    'dbfilename interop.rdb',
    'logfile ""',
    'daemonize no'
) -join "`r`n"
[System.IO.File]::WriteAllText($conf, $confText, [System.Text.Encoding]::ASCII)

$serverProc = $null
try {
    # 本机 redis-server 是 MSYS2 构建：**不认 Windows 绝对路径**（会把 cwd 前缀拼到参数前面），
    # 因此传相对配置名 + `-WorkingDirectory`，让它在临时目录里自行解析（已实测）。
    $serverProc = Start-Process -FilePath $server -ArgumentList @('redis.conf') `
        -WorkingDirectory $script:tmp `
        -RedirectStandardOutput (Join-Path $script:tmp 'server.out.txt') `
        -RedirectStandardError (Join-Path $script:tmp 'server.err.txt') `
        -PassThru -NoNewWindow
    Write-Line "== 临时实例 127.0.0.1:$script:port（临时目录 $script:tmp） =="

    $ready = $false
    for ($i = 0; $i -lt 100; $i++) {
        Start-Sleep -Milliseconds 100
        try {
            $pong = Invoke-RedisCliRaw -Arguments @('PING')

            if ([System.Text.Encoding]::ASCII.GetString($pong) -eq 'PONG') { $ready = $true; break }
        } catch {
            if ($serverProc.HasExited) { break }
        }
    }
    if (-not $ready) {
        $err = Join-Path $script:tmp 'server.err.txt'
        $detail = if (Test-Path -LiteralPath $err) { Get-Content -LiteralPath $err -Raw -Encoding UTF8 } else { '' }
        throw "临时 redis-server 10s 内未就绪（端口 $script:port）：$detail"
    }

    $conn = "server=127.0.0.1:$script:port;db=0"

    # ---------------- 1. Zig 写 → Zig 读（新进程）
    $w = Invoke-Zredis -Label 'zig-write' -Arguments @('interop', 'write', $conn, $Prefix)
    if ($w.ExitCode -ne 0) { throw "interop write 失败：$($w.Text)" }
    $v = Invoke-Zredis -Label 'zig-verify' -Arguments @('interop', 'verify', $conn, $Prefix)
    Assert-Ok -Name 'zig.write-then-verify' -Ok (($v.Text -match 'RESULT=OK') -and ($v.Text -match 'FAILED=0')) -Detail $v.Text

    # ---------------- 2. Zig 写 → redis-cli 读（外部口径，逐字节）
    Assert-Ok 'ext.str.plain' (([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('GET', (K 'str:plain')))) -eq $spec.plain)) '值不等'
    Assert-Ok 'ext.str.utf8' (([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('GET', (K 'str:utf8')))) -eq $spec.utf8)) 'UTF-8 字节不等'
    $binGot = Invoke-RedisCliRaw @('GET', (K 'str:bin'))
    Assert-Ok 'ext.str.binary' (Test-BytesEqual -Actual $binGot -Expected $binValue) ("期望 $((ConvertTo-Hex $binValue))，实际 $((ConvertTo-Hex $binGot))")
    $ttlText = [System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('TTL', (K 'str:ttl'))))
    $ttlVal = 0
    [void][int]::TryParse($ttlText, [ref]$ttlVal)
    Assert-Ok 'ext.str.ttl' ($ttlVal -gt 0 -and $ttlVal -le $spec.ttlSec) "TTL=$ttlText"
    Assert-Ok 'ext.num.counter' (([System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('GET', (K 'num:counter')))) -eq $spec.counter)) '计数器不是 3'
    Assert-Ok 'ext.val.bool' (([System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('GET', (K 'val:bool')))) -eq $spec.bool)) '布尔不是 C# 口径 True'
    $timeText = [System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('GET', (K 'val:time'))))
    Assert-Ok 'ext.val.time' ($timeText -match '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$') "时间=$timeText"
    Assert-Ok 'ext.hash' (([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('HGET', (K 'hash'), 'f1'))) -eq $spec.hashF1) -and ([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('HGET', (K 'hash'), 'f2'))) -eq $spec.hashF2)) 'HGET 不等'
    $listText = [System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('LRANGE', (K 'list'), '0', '-1')))
    Assert-Ok 'ext.list' (($listText -replace "`r`n", "`n") -eq ($spec.list -join "`n")) "LRANGE=[$listText]"
    $setText = [System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('SMEMBERS', (K 'set'))))
    $setGot = @($setText -split "`r?`n" | Where-Object { $_ -ne '' } | Sort-Object)
    $setWant = @($spec.set | Sort-Object)
    Assert-Ok 'ext.set' (($setGot -join ',') -eq ($setWant -join ',')) "SMEMBERS=[$setText]"
    Assert-Ok 'ext.zset.m1' (([System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('ZSCORE', (K 'zset'), 'm1'))) -eq $spec.z1)) 'm1 score 文本不等'
    Assert-Ok 'ext.zset.m2' (([System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('ZSCORE', (K 'zset'), 'm2'))) -eq $spec.z2)) 'm2 score 文本不等'
    Assert-Ok 'ext.queue' (([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('LINDEX', (K 'queue'), '0'))) -eq $spec.queue)) '队列首元素不等'
    $delayLines = @([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('ZRANGE', (K 'delay'), '0', '-1', 'WITHSCORES'))) -split "`r?`n" | Where-Object { $_ -ne '' })
    $delayOk = $false
    if ($delayLines.Count -eq 2 -and $delayLines[0] -eq $spec.delay) {
        $score = 0.0
        if ([double]::TryParse($delayLines[1], [ref]$score)) {
            $expect = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() + $spec.delaySec
            $delayOk = [math]::Abs($score - $expect) -le 300
        }
    }
    Assert-Ok 'ext.queue.delay' $delayOk "ZRANGE=[$($delayLines -join ',')]"
    Assert-Ok 'ext.queue.reliable' (([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('LINDEX', (K 'reliable'), '0'))) -eq $spec.reliable)) '可靠队列首元素不等'
    $streamLines = @([System.Text.Encoding]::UTF8.GetString((Invoke-RedisCliRaw @('XRANGE', (K 'stream'), '-', '+', 'COUNT', '1'))) -split "`r?`n" | Where-Object { $_ -ne '' })
    $streamOk = $streamLines.Count -eq 3 -and $streamLines[0] -match '^\d+-\d+$' -and $streamLines[1] -eq $spec.streamF -and $streamLines[2] -eq $spec.streamV
    Assert-Ok 'ext.stream' $streamOk "XRANGE=[$($streamLines -join '|')]"
    $lockText = [System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('GET', (K 'str:plain:lock'))))
    Assert-Ok 'ext.lock' ($lockText -eq "$($spec.lockTok)|$($spec.lockMs)") "锁值=[$lockText]"

    # ---------------- 3. redis-cli 写（--pipe 灌精确 RESP）→ Zig 读
    $clean = Invoke-Zredis -Label 'zig-clean' -Arguments @('interop', 'clean', $conn, $Prefix)
    if ($clean.ExitCode -ne 0) { throw "interop clean 失败：$($clean.Text)" }

    $lockValue = "$($spec.lockTok)|$($spec.lockMs)"
    $commands = @(
        @('SET', (K 'str:plain'), $spec.plain),
        @('SET', (K 'str:utf8'), $spec.utf8),
        @('SET', (K 'str:bin'), $binValue),
        @('SET', (K 'str:ttl'), $spec.ttl, 'EX', "$($spec.ttlSec)"),
        @('SET', (K 'num:counter'), $spec.counter),
        @('SET', (K 'val:bool'), $spec.bool),
        @('SET', (K 'val:time'), '2026-11-18 09:05:07.123'),
        @('HSET', (K 'hash'), 'f1', $spec.hashF1, 'f2', $spec.hashF2),
        @('RPUSH', (K 'list'), $spec.list[0], $spec.list[1], $spec.list[2]),
        @('SADD', (K 'set'), $spec.set[0], $spec.set[1]),
        @('ZADD', (K 'zset'), $spec.z1, 'm1', $spec.z2, 'm2'),
        @('RPUSH', (K 'queue'), $spec.queue),
        @('ZADD', (K 'delay'), "$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() + $spec.delaySec)", $spec.delay),
        @('RPUSH', (K 'reliable'), $spec.reliable),
        @('XADD', (K 'stream'), '*', $spec.streamF, $spec.streamV),
        @('SET', (K 'str:plain:lock'), $lockValue)
    )
    $pipeFile = Join-Path $script:tmp 'external.resp'
    New-PipeFile -Path $pipeFile -Commands $commands
    Invoke-RedisPipe -PipeFile $pipeFile

    $v2 = Invoke-Zredis -Label 'zig-verify-external' -Arguments @('interop', 'verify', $conn, $Prefix)
    Assert-Ok -Name 'zig.read-external' -Ok (($v2.Text -match 'RESULT=OK') -and ($v2.Text -match 'FAILED=0')) -Detail $v2.Text

    $clean2 = Invoke-Zredis -Label 'zig-clean2' -Arguments @('interop', 'clean', $conn, $Prefix)
    Assert-Ok -Name 'zig.clean' -Ok ($clean2.Text -match 'CLEANED=') -Detail $clean2.Text

    $leftover = [System.Text.Encoding]::ASCII.GetString((Invoke-RedisCliRaw @('DBSIZE')))
    Assert-Ok -Name 'ext.dbsize-zero' -Ok ($leftover.Trim() -eq '0') -Detail "DBSIZE=$leftover"

    if ($script:failures -eq 0) {
        Write-Line "RESULT=PASS（$script:checks 项检查）"
        exit 0
    }
    Write-Line "RESULT=FAIL（$script:checks 项检查，失败 $script:failures）"
    exit 1
} catch {
    Write-Line "RESULT=FAIL: $($_.Exception.Message)"
    exit 1
} finally {
    if ($serverProc -and -not $serverProc.HasExited) {
        Stop-Process -Id $serverProc.Id -Force -ErrorAction SilentlyContinue
    }
    if (-not $KeepServer) {
        Remove-Item -Recurse -Force -LiteralPath $script:tmp -ErrorAction SilentlyContinue
    } else {
        Write-Line "保留临时目录：$script:tmp"
    }
}
