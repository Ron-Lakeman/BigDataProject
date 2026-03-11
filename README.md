# BigDataProject

## Quick setup (Windows + PowerShell)

Use **one environment only** for this repo (recommended: `venv_bigdata`).

### If `venv_bigdata` already exists

```powershell
.\venv_bigdata\Scripts\Activate.ps1
.\venv_bigdata\Scripts\python.exe -m ensurepip --upgrade
.\venv_bigdata\Scripts\python.exe -m pip install --upgrade pip
.\venv_bigdata\Scripts\python.exe -m pip install -r requirements.txt
```

### 1) Create and activate virtual environment

```powershell
py -3.10 -m venv venv_bigdata
.\venv_bigdata\Scripts\Activate.ps1
```

If PowerShell blocks activation:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

### 2) Install dependencies

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3) Select this environment in VS Code

- `Ctrl+Shift+P` -> **Python: Select Interpreter**
- Choose: `venv_bigdata\Scripts\python.exe`

### 4) Run database initialization

From project root:

```powershell
python src\config\initializeDB.py
```

If that fails because of path issues, run from inside `src\config`:

```powershell
cd src\config
python initializeDB.py
```

### 5) One-time Windows Spark prerequisites (for notebooks)

Install Java 17 (recommended):

```powershell
winget install --id EclipseAdoptium.Temurin.17.JDK -e --accept-package-agreements --accept-source-agreements
```

Then restart VS Code so the new Java installation is picked up by notebook kernels.

Download local Hadoop Windows tools used by Spark:

```powershell
New-Item -ItemType Directory -Path .tools\hadoop\bin -Force | Out-Null
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" -OutFile ".tools\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile ".tools\hadoop\bin\hadoop.dll"
```

Quick check:

```powershell
java -version
Get-Item .tools\hadoop\bin\winutils.exe, .tools\hadoop\bin\hadoop.dll
```

## Notebook prerequisites (PySpark + Delta)

This project uses PySpark with Delta Lake in notebooks.

1. Install Java (JDK 11 or 17).
2. Set `JAVA_HOME` to your JDK install folder.
3. Restart VS Code after changing environment variables.

The notebook already sets:

- `SPARK_LOCAL_IP=127.0.0.1`
- Delta package `io.delta:delta-spark_2.12:3.2.0`

### Cross-platform note for teammates

- The setup cell in `src/processing/ingest_to_bronze.ipynb` is now platform-aware.
- On macOS/Linux, Windows-specific Hadoop/winutils logic is skipped automatically.
- On Windows, the notebook uses local tools in `.tools/hadoop/bin` (`winutils.exe` and `hadoop.dll`) when present.

## Common confusion to avoid

- Don’t mix interpreters (`venv_bigdata` and Conda) in the same run.
- Make sure notebook kernel is the same `venv_bigdata` interpreter.