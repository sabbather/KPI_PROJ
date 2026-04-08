Deployment (Windows, workstation)
=================================

1) Skopiuj projekt  
   Wystarczą pliki: `streamlit_app.py`, `requirements.txt`, `.env` (oraz ewentualnie `trashbin` jeśli chcesz odzyskać pliki).

2) Zainstaluj Pythona 3.11+  
   Upewnij się, że `python` jest w PATH.

3) Utwórz i aktywuj wirtualne środowisko
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

4) Zainstaluj zależności
   ```powershell
   pip install -r requirements.txt
   ```

5) Uzupełnij `.env`  
   Przenieś lub wpisz wartości: `WRIKE_API_KEY`, `WRIKE_CLIENT_PROJECTS_FOLDER_ID`, opcjonalnie `WRIKE_BASE_URL`, `WRIKE_CORE_TASK_TYPE_ID`, `WRIKE_CORE_PROJECT_TYPE_ID`, `WRIKE_PLANNED_EFFORT_FIELD_ID`, `WRIKE_COMPLETED_STATUS_ID`.

6) Uruchom aplikację Streamlit
   ```powershell
   streamlit run streamlit_app.py
   ```
   Domyślnie: http://localhost:8501

Notatki:
- Jeśli potrzebujesz plików z `trashbin/`, przenieś je z powrotem do katalogu głównego przed instalacją.
- Do aktualizacji zależności po zmianach użyj `pip freeze > requirements.txt` (opcjonalnie).

## Skróty start/stop (ciche)
- Start w tle: `.\start_app.ps1`
- Stop (zatrzymuje streamlit/python tej aplikacji): `.\stop_app.ps1`
