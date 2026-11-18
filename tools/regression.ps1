#requires -Version 5.1
<#
.SYNOPSIS
    Pek.ZRedis 一键回归：格式检查 → 构建 → 单元测试 → 集成测试 → 报告。

.DESCRIPTION
    - 退出码 0/1，可直接接 CI；
    - 报告落 artifacts\regression\report-<时间戳>.txt；
    - 单元 / 集成测试分别用独立缓存目录跑，保证每次都真实执行并拿到测试数；
    - 测试数低于基线视为失败（基线随新增用例抬高）。

.NOTES
    PS 5.1 兼容：
    - 外部命令 stderr 在 $ErrorActionPreference='Stop' 下会被当作终止错误，调用时临时切 'Continue'；
    - 重定向的 stderr 会变成 ErrorRecord，统一取 Exception.Message，避免输出 RemoteException 噪声。
#>
[CmdletBinding()]
param(
    # 单元测试基线（新增用例后必须同步抬高）
    [int]$MinUnit = 46,
    # 集成测试基线（新增用例后必须同步抬高）
    [int]$MinIntegration = 26
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$script:report = $null

function Write-Line {
    param([string]$Text)
    Write-Host $Text
    if ($script:report) { Add-Content -LiteralPath $script:report -Value $Text -Encoding UTF8 }
}

function Invoke-External {
    param(
        [string]$Label,
        [string]$Exe,
        [string[]]$Arguments
    )
    Write-Line "== $Label =="
    $captured = New-Object System.Collections.Generic.List[string]
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $code = 1
    try {
        & $Exe @Arguments 2>&1 | ForEach-Object {
            $line = if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" }
            $captured.Add($line)
            Write-Host $line
            Add-Content -LiteralPath $script:report -Value $line -Encoding UTF8
        }
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -ne 0) {
        throw "$Label 失败（exit $code）"
    }
    return $captured.ToArray()
}

function Assert-TestCount {
    param(
        [string]$Label,
        [string[]]$Lines,
        [int]$Minimum
    )
    $text = $Lines -join "`n"
    $match = [regex]::Match($text, '(\d+)/(\d+) tests passed')
    if (-not $match.Success) {
        throw "$Label 未输出测试统计，无法确认结果（禁止静默通过）"
    }
    $passed = [int]$match.Groups[1].Value
    $total = [int]$match.Groups[2].Value
    if ($passed -lt $Minimum) {
        throw "$Label 测试数低于基线：$passed < $Minimum"
    }
    if ($passed -ne $total) {
        throw "$Label 存在失败用例：$passed/$total"
    }
    Write-Line "PASS: $Label $passed/$total（基线 $Minimum）"
}

<#
.SYNOPSIS
    TLS 正向往返（PING / SET / GET）：起最小 TLS RESP 服务端，用 CLI 走真实 TLS 连接。
.DESCRIPTION
    缺口具（python / 证书）时显式打 SKIP + 原因，**不计 PASS**，不允许静默跳过。
#>
function Invoke-TlsRoundTrip {
    param(
        [string]$ExePath,
        [string]$RootPath
    )
    $serverScript = Join-Path $RootPath 'tests\support\tls\tls_redis_server.py'
    $cert = Join-Path $RootPath 'tests\support\tls\cert.pem'
    $key = Join-Path $RootPath 'tests\support\tls\key.pem'
    foreach ($path in @($serverScript, $cert, $key)) {
        if (-not (Test-Path -LiteralPath $path)) {
            Write-Line "SKIP: TLS 正向用例缺少夹具（$path）"
            return
        }
    }
    if (-not (Test-Path -LiteralPath $ExePath)) {
        Write-Line "SKIP: TLS 正向用例需要 CLI 产物（$ExePath）"
        return
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if (-not $python) {
        Write-Line "SKIP: TLS 正向用例需要 python（ssl 模块），本机未找到 python"
        return
    }

    # 取一个空闲端口（先绑 0 再释放，避免与在跑的服务撞口）
    $probe = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
    $probe.Start()
    $port = ([System.Net.IPEndPoint]$probe.LocalEndpoint).Port
    $probe.Stop()

    $logDir = Join-Path $RootPath 'artifacts\regression'
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $outLog = Join-Path $logDir "tls-server-$port.out.log"
    $errLog = Join-Path $logDir "tls-server-$port.err.log"

    Write-Line "== TLS 正向（127.0.0.1:$port） =="
    $server = Start-Process -FilePath $python.Source `
        -ArgumentList @($serverScript, "$port", $cert, $key) `
        -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog
    try {
        $ready = $false
        for ($i = 0; $i -lt 100; $i++) {
            Start-Sleep -Milliseconds 100
            if (Test-Path -LiteralPath $outLog) {
                $text = Get-Content -LiteralPath $outLog -Raw -Encoding UTF8
                if ($text -and $text.Contains('ready')) { $ready = $true; break }
            }
            if ($server.HasExited) { break }
        }
        if (-not $ready) {
            $stderr = if (Test-Path -LiteralPath $errLog) { Get-Content -LiteralPath $errLog -Raw -Encoding UTF8 } else { '' }
            throw "TLS 夹具服务端 10s 内未就绪（端口 $port，stderr: $stderr）"
        }

        $conn = "server=127.0.0.1:$port;Tls=1;TlsInsecure=1;timeout=3000"
        $pingLines = Invoke-External -Label 'TLS PING' -Exe $ExePath -Arguments @('ping', $conn)
        if (($pingLines -join "`n") -notmatch 'RESULT=OK') {
            throw "TLS PING 未返回 RESULT=OK：$($pingLines -join ' ')"
        }
        $setLines = Invoke-External -Label 'TLS SET' -Exe $ExePath -Arguments @('set', $conn, 'hello', 'world')
        if (($setLines -join "`n") -notmatch 'RESULT=OK') {
            throw "TLS SET 未返回 RESULT=OK：$($setLines -join ' ')"
        }
        $getLines = Invoke-External -Label 'TLS GET' -Exe $ExePath -Arguments @('get', $conn, 'hello')
        $getText = $getLines -join "`n"
        if ($getText -notmatch 'RESULT=OK' -or $getText -notmatch 'VALUE=world') {
            throw "TLS GET 未取回 world：$($getLines -join ' ')"
        }
        Write-Line "PASS: TLS 正向 PING / SET / GET（127.0.0.1:$port）"
    } finally {
        if ($server -and -not $server.HasExited) {
            Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

<#
.SYNOPSIS
    跨语言严格对拍（调 tools\interop-strict.ps1，自建临时 redis 实例）。
.DESCRIPTION
    缺 redis-server/redis-cli 时脚本会自己打 `SKIP: ...` 并退出 0——这里原样透传，不算 PASS。
#>
function Invoke-InteropStrict {
    param([string]$RootPath)
    $script = Join-Path $RootPath 'tools\interop-strict.ps1'
    if (-not (Test-Path -LiteralPath $script)) {
        Write-Line "SKIP: 缺少 tools\interop-strict.ps1"
        return
    }
    Write-Line '== 跨语言严格对拍（tools\interop-strict.ps1） =='
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $code = 1
    try {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $script -NoBuild 2>&1 | ForEach-Object {
            $line = if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" }
            Write-Line $line
        }
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -ne 0) {
        throw "跨语言严格对拍失败（exit $code）"
    }
}

try {
    Push-Location $root
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $reportDir = Join-Path $root 'artifacts\regression'
    New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
    $script:report = Join-Path $reportDir "report-$stamp.txt"

    Write-Line "Pek.ZRedis 回归报告 $stamp"
    Write-Line "目录: $root"

    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $branch = & git rev-parse --abbrev-ref HEAD 2>$null
        if ($LASTEXITCODE -eq 0) { Write-Line "分支: $branch" }
        $head = & git rev-parse --short HEAD 2>$null
        if ($LASTEXITCODE -eq 0) { Write-Line "HEAD: $head" }
    } finally {
        $ErrorActionPreference = $previous
    }

    Invoke-External -Label 'zig fmt --check' -Exe 'zig' -Arguments @('fmt', '--check', 'src', 'build.zig', 'tests') | Out-Null
    Invoke-External -Label 'zig build' -Exe 'zig' -Arguments @('build') | Out-Null

    # 独立缓存：避免命中缓存导致「测试没跑也算通过」
    $cache = Join-Path $root '.zig-cache-regression'
    if (Test-Path $cache) { Remove-Item -Recurse -Force $cache }

    $unitLines = Invoke-External -Label 'zig build unit' -Exe 'zig' -Arguments @('build', 'unit', '--summary', 'all', '--cache-dir', $cache)
    Assert-TestCount -Label '单元测试' -Lines $unitLines -Minimum $MinUnit

    if (Test-Path $cache) { Remove-Item -Recurse -Force $cache }
    $integrationLines = Invoke-External -Label 'zig build integration' -Exe 'zig' -Arguments @('build', 'integration', '--summary', 'all', '--cache-dir', $cache)
    Assert-TestCount -Label '集成测试' -Lines $integrationLines -Minimum $MinIntegration

    Invoke-TlsRoundTrip -ExePath (Join-Path $root 'zig-out\bin\zredis.exe') -RootPath $root

    Invoke-InteropStrict -RootPath $root

    Write-Line 'RESULT=PASS'
    exit 0
} catch {
    $message = "RESULT=FAIL: $($_.Exception.Message)"
    Write-Host $message -ForegroundColor Red
    if ($script:report) { Add-Content -LiteralPath $script:report -Value $message -Encoding UTF8 }
    exit 1
} finally {
    Pop-Location
}





