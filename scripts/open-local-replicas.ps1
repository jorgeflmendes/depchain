param(
  [Parameter(Mandatory = $true)]
  [string]$ProjectDir,

  [Parameter(Mandatory = $false)]
  [string]$ConfigPath = "config/config.yaml"
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
