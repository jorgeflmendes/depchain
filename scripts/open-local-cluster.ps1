param(
  [Parameter(Mandatory = $true)]
  [string]$ProjectDir,

  [Parameter(Mandatory = $false)]
  [string]$ConfigPath = "config/config.yaml"
)

$ErrorActionPreference = "Stop"

$runnerScript = Join-Path $PSScriptRoot "run-maven-role.ps1"
$serverIds = @("server1", "server2", "server3", "server4")

foreach ($serverId in $serverIds) {
  Start-Process -FilePath "powershell.exe" -WorkingDirectory $ProjectDir -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $runnerScript,
    "-ProjectDir",
    $ProjectDir,
    "-WindowTitle",
    "depchain-$serverId",
    "-Role",
    "server",
    "-ReplicaId",
    $serverId,
    "-ConfigPath",
    $ConfigPath
  )
}

Start-Process -FilePath "powershell.exe" -WorkingDirectory $ProjectDir -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy",
  "Bypass",
  "-File",
  $runnerScript,
  "-ProjectDir",
  $ProjectDir,
  "-WindowTitle",
  "depchain-client",
  "-Role",
  "client",
  "-ConfigPath",
  $ConfigPath
)
