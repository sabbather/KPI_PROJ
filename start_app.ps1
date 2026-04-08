$ErrorActionPreference = "Stop"

Push-Location $PSScriptRoot

$venv = ".\.venv\Scripts\Activate.ps1"
if (Test-Path $venv) { & $venv }

# start Streamlit quietly
$cmd = "streamlit run streamlit_app.py --server.headless true"
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoProfile -WindowStyle Hidden -Command `$ErrorActionPreference='Stop'; $cmd='$cmd'; Push-Location '$PSScriptRoot'; & $cmd" -WindowStyle Hidden

Write-Output "Streamlit start command launched in background."

Pop-Location
