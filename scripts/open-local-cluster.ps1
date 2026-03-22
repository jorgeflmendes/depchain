param(
  [Parameter(Mandatory = $true)]
  [string]$ProjectDir,

  [Parameter(Mandatory = $false)]
  [string]$ConfigPath = "config/config.yaml",

  [Parameter(Mandatory = $true)]
  [string]$ClientId
)

$ErrorActionPreference = "Stop"
$serverIds = @("server1", "server2", "server3", "server4")

foreach ($serverId in $serverIds) {
  $serverArgs = "$serverId $ConfigPath"
  $serverCommand = "`$host.UI.RawUI.WindowTitle = 'depchain-$serverId'; Set-Location -LiteralPath '$ProjectDir'; Write-Host 'Running: mvn -q exec:java@server -Dexec.args=""$serverArgs""' -ForegroundColor Cyan; mvn -q exec:java@server ""-Dexec.args=$serverArgs"""
  Start-Process -FilePath "powershell.exe" -WorkingDirectory $ProjectDir -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    $serverCommand
  )
}

$clientArgs = "--config $ConfigPath --client-id $ClientId"
$clientCommand = "`$host.UI.RawUI.WindowTitle = 'depchain-client-$ClientId'; Set-Location -LiteralPath '$ProjectDir'; Write-Host 'Running: mvn -q exec:java@client -Dexec.args=""$clientArgs""' -ForegroundColor Cyan; mvn -q exec:java@client ""-Dexec.args=$clientArgs"""
Start-Process -FilePath "powershell.exe" -WorkingDirectory $ProjectDir -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy",
  "Bypass",
  "-Command",
  $clientCommand
)
