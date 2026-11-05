# =============================================================================
# spark_run.ps1  PySpark wrapper for Windows (ecommerce-data-platform)
#
# WHY THIS EXISTS:
#   On Windows, PySpark's JVM tries to launch Python worker processes using the
#   PYSPARK_PYTHON env var. If this var is unset and no system-wide 'python'
#   exists, Spark hangs silently instead of throwing an error.
#
# USAGE:
#   .\scripts\spark_run.ps1                          # opens interactive python
#   .\scripts\spark_run.ps1 src\jobs\etl_job.py     # run a PySpark script
#   .\scripts\spark_run.ps1 -c "print('hello')"     # inline python expression
#   .\scripts\spark_run.ps1 -Test                   # quick Spark smoke test
#
# =============================================================================

param(
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$UserArgs,

    [switch]$Test   # run a built-in smoke test instead of a user script
)

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$EnvFile     = Join-Path $ProjectRoot ".env"
$VenvPython  = Join-Path $ProjectRoot "venv\Scripts\python.exe"

# ── 1. Verify venv python exists ─────────────────────────────────────────────
if (-not (Test-Path $VenvPython)) {
    Write-Error "venv python not found at: $VenvPython"
    Write-Error "Run:  python -m venv venv  then activate it and  pip install -r requirements.txt"
    exit 1
}

# ── 2. Load .env (key=value pairs only; skip comments and blanks) ─────────────
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith('#') -and $line -match '^([^=]+)=(.*)$') {
            $key   = $Matches[1].Trim()
            $value = $Matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $value, 'Process')
        }
    }
    Write-Host "[spark_run] Loaded .env" -ForegroundColor DarkGray
} else {
    Write-Warning "[spark_run] .env not found at $EnvFile - using existing environment."
}

# ── 3. Enforce Spark Python vars (override even if .env had wrong values) ─────
$env:PYSPARK_PYTHON        = "C:\Users\navya\Desktop\ecommerce-data-platform\python_wrapper.bat"
$env:PYSPARK_DRIVER_PYTHON = $VenvPython
$env:HADOOP_HOME           = "C:\hadoop"

Write-Host "[spark_run] PYSPARK_PYTHON        = $env:PYSPARK_PYTHON" -ForegroundColor Cyan
Write-Host "[spark_run] PYSPARK_DRIVER_PYTHON = $env:PYSPARK_DRIVER_PYTHON" -ForegroundColor Cyan
Write-Host "[spark_run] HADOOP_HOME           = $env:HADOOP_HOME" -ForegroundColor Cyan
Write-Host "[spark_run] JAVA_HOME             = $env:JAVA_HOME" -ForegroundColor Cyan
Write-Host ""

# ── 4. Smoke test mode ────────────────────────────────────────────────────────
if ($Test) {
    Write-Host "[spark_run] Running Spark smoke test..." -ForegroundColor Yellow
    $TestScript = @"
import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PYSPARK_PYTHON'] = r'C:\Users\navya\Desktop\ecommerce-data-platform\venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\navya\Desktop\ecommerce-data-platform\venv\Scripts\python.exe'
from pyspark.sql import SparkSession
s = (SparkSession.builder
       .master('local[2]')
       .appName('smoke-test')
       .config('spark.ui.enabled', 'false')
       .config('spark.driver.extraJavaOptions', '-Dhadoop.home.dir=C:/hadoop')
       .config('spark.executor.extraJavaOptions', '-Dhadoop.home.dir=C:/hadoop')
       .getOrCreate())
print('Spark OK:', s.version)
df = s.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'val'])
df.show()
s.stop()
print('Smoke test PASSED.')
"@
    & $VenvPython -c $TestScript
    exit $LASTEXITCODE
}

# ── 5. Pass-through: run whatever the user provided ───────────────────────────
if ($UserArgs.Count -eq 0) {
    Write-Host "[spark_run] No script specified - opening interactive Python REPL." -ForegroundColor Yellow
    & $VenvPython
} else {
    & $VenvPython @UserArgs
}

exit $LASTEXITCODE
