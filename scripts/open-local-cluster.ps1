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
  $serverCommand = @"
`$host.UI.RawUI.WindowTitle = 'depchain-$serverId'
Set-Location -LiteralPath '$ProjectDir'
`$mavenArgs = @('-q', 'exec:java@server', "-Dexec.args=$serverArgs")
Write-Host "Running: mvn `$(`$mavenArgs -join ' ')" -ForegroundColor Cyan
& mvn @mavenArgs
"@
  $serverEncodedCommand = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($serverCommand))
  Start-Process -FilePath "powershell.exe" -WorkingDirectory $ProjectDir -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy",
    "Bypass",
    "-EncodedCommand",
    $serverEncodedCommand
  )
}

$clientArgs = "--config $ConfigPath --client-id $ClientId"
$clientCommand = @"
`$host.UI.RawUI.WindowTitle = 'depchain-client-$ClientId'
Set-Location -LiteralPath '$ProjectDir'
`$mavenArgs = @('-q', 'exec:java@client', "-Dexec.args=$clientArgs")
Write-Host "Running: mvn `$(`$mavenArgs -join ' ')" -ForegroundColor Cyan
& mvn @mavenArgs
"@
$clientEncodedCommand = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($clientCommand))
Start-Process -FilePath "powershell.exe" -WorkingDirectory $ProjectDir -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy",
  "Bypass",
  "-EncodedCommand",
  $clientEncodedCommand
)
