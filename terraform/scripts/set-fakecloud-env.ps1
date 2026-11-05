# scripts/set-fakecloud-env.ps1
# Source this to configure AWS CLI for fakecloud:
#   . .\scripts\set-fakecloud-env.ps1

$env:AWS_ACCESS_KEY_ID     = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION    = "us-east-1"
$env:AWS_ENDPOINT_URL      = "http://localhost:4566"

Write-Host "fakecloud env vars set:" -ForegroundColor Green
Write-Host "  AWS_ACCESS_KEY_ID     = $env:AWS_ACCESS_KEY_ID"
Write-Host "  AWS_SECRET_ACCESS_KEY = (hidden)"
Write-Host "  AWS_DEFAULT_REGION    = $env:AWS_DEFAULT_REGION"
Write-Host "  AWS_ENDPOINT_URL      = $env:AWS_ENDPOINT_URL"
