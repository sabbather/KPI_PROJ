$ErrorActionPreference = "SilentlyContinue"

# Kill streamlit processes started for this app
Get-Process streamlit, python | Where-Object { $_.Path -like "*streamlit*" -or $_.StartInfo.Arguments -like "*streamlit_app.py*" } | Stop-Process -Force

Write-Output "Streamlit (if running) has been stopped."
