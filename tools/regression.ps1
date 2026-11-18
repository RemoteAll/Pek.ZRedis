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
    [int]$MinUnit = 36,
    # 集成测试基线（新增用例后必须同步抬高）
    [int]$MinIntegration = 21
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

