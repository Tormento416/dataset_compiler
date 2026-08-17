#!/usr/bin/env bash
# Runs Dataset Compiler from source on Linux/macOS: creates the venv, installs
# dependencies (including tkinter at the OS level if missing), and launches the GUI.
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

"$VENV_PY" downloader_gui.py
