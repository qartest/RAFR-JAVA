
$ErrorActionPreference = "Stop"

# Путь к корню проекта — замени на свой
$ProjectDir = "C:\WORK_HARD\GIT\MyRaft2\MyRaft"

# Единый конфигурационный файл для всех нод
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

foreach ($Id in 1..5) {
    $Title = "Raft node $Id"

    $Arguments = @(
        "-jar"
        "`"$($Jar.FullName)`""
        "--spring.config.additional-location=file:$ConfigFile"
        "--raft.node-id=$Id"
    ) -join " "

    Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @(
            "-NoExit"
            "-Command"
            "`$Host.UI.RawUI.WindowTitle = '$Title'; java $Arguments"
        )

    Start-Sleep -Milliseconds 500
}

Write-Host "Started launch commands for all five nodes."
