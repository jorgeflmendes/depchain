param(
  [Parameter(Mandatory = $true)]
  [string]$ProjectDir,

  [Parameter(Mandatory = $true)]
  [string]$WindowTitle,

  [Parameter(Mandatory = $true)]
  [ValidateSet("server", "client")]
  [string]$Role,

  [Parameter(Mandatory = $false)]
  [string]$ReplicaId,

  [Parameter(Mandatory = $true)]
  [string]$ConfigPath
)

$ErrorActionPreference = "Stop"
$Host.UI.RawUI.WindowTitle = $WindowTitle

Set-Location -LiteralPath $ProjectDir

$goal = if ($Role -eq "server") { "exec:java@server" } else { "exec:java@client" }
$execArgs = if ($Role -eq "server") { "$ReplicaId $ConfigPath" } else { $ConfigPath }
$mavenArgs = @("-q", $goal, "-Dexec.args=$execArgs")

Write-Host "Running: mvn $($mavenArgs -join ' ')" -ForegroundColor Cyan
& mvn @mavenArgs

if ($LASTEXITCODE -ne 0) {
  Write-Host "Process exited with code $LASTEXITCODE" -ForegroundColor Red
} else {
  Write-Host "Process finished successfully." -ForegroundColor Green
}
