#!/usr/bin/env bash
# Build script for Linux/macOS. Windows equivalent: build.bat
# PyInstaller does not cross-compile - this must be run ON the target OS
# to produce that OS's binary.
set -e
cd "$(dirname "$0")"
source ensure_tk.sh
ensure_tk

VENV_PY="venv/bin/python"
if [ ! -x "$VENV_PY" ]; then
    echo "venv not found. Creating it..."
    python3 -m venv venv
    venv/bin/pip install -r requirements.txt
fi

"$VENV_PY" -m PyInstaller --noconfirm --onedir --windowed --name "DatasetDownloader" "downloader_gui.py"
echo "Build complete: dist/DatasetDownloader/"
