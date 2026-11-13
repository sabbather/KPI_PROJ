Deployment (Windows, workstation)
=================================

1) Skopiuj projekt  
   Wystarczą pliki: `streamlit_app.py`, `VERSION`, `requirements.txt`, `.env` (oraz ewentualnie `trashbin` jeśli chcesz odzyskać pliki).

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

## Wersjonowanie
Wersja aplikacji jest przechowywana w pliku `VERSION` (format semantyczny `MAJOR.MINOR.PATCH`) i pokazywana jako znacznik `v...` w prawym górnym rogu strony głównej oraz na ekranie logowania.

- Podniesienie wersji: zmień zawartość pliku `VERSION`, a następnie zrób commit i tag:
  ```bash
  git add VERSION
  git commit -m "Bump version to 1.1.0"
  git tag -a v1.1.0 -m "Wersja 1.1.0"
  git push origin master --tags
  ```
- Numer wersji można też nadpisać zmienną środowiskową `APP_VERSION` (np. w `.env` lub `docker-compose.yml`); ma ona pierwszeństwo przed plikiem `VERSION`.
- Docker kopiuje `VERSION` do obrazu, więc znacznik działa również w kontenerze.

