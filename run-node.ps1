
param(
    [Parameter(Mandatory = $true)]
    [int]$NodeId
)

$ErrorActionPreference = "Stop"

# Путь к корню проекта
$ProjectDir = "C:\WORK_HARD\GIT\MyRaft2\MyRaft"

# Конфигурационный файл
$ConfigFile = "C:\WORK_HARD\GIT\MyRaft2\MyRaft\src\main\resources\application.yml"

Set-Location $ProjectDir

if (-not (Test-Path $ConfigFile)) {
    throw "Config file not found: $ConfigFile"
}

Write-Host "Building project..."
& .\gradlew.bat bootJar

if ($LASTEXITCODE -ne 0) {
    throw "Gradle build failed"
}

$Jar = Get-ChildItem "$ProjectDir\build\libs\*.jar" |
    Where-Object { $_.Name -notlike "*-plain.jar" } |
    Select-Object -First 1

if (-not $Jar) {
    throw "Executable JAR not found in build\libs"
}

Write-Host "JAR: $($Jar.FullName)"
Write-Host "Config: $ConfigFile"
Write-Host "Starting Raft node $NodeId..."

$Title = "Raft node $NodeId"

Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList @(
        "-NoExit",
        "-Command",
        "`$Host.UI.RawUI.WindowTitle = '$Title'; java -jar `"$($Jar.FullName)`" `"--spring.config.additional-location=file:$ConfigFile`" `"--raft.node-id=$NodeId`""
    )

Write-Host "Started launch command for node $NodeId."
