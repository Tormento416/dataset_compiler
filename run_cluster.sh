#!/usr/bin/env bash
# Starts a coordinator and 3 local writers on Linux/macOS. Windows equivalent: run_cluster.bat
set -e

VENV_PY="venv/bin/python"
if [ ! -x "$VENV_PY" ]; then
    echo "venv not found. Creating it..."
    python3 -m venv venv
    venv/bin/pip install -r requirements.txt
fi

echo "Starting Coordinator Microservice..."
"$VENV_PY" coordinator.py &
sleep 2

echo "Starting 3 Writer instances..."
"$VENV_PY" writer.py &
"$VENV_PY" writer.py &
"$VENV_PY" writer.py &

echo "Cluster started! Press Ctrl+C to stop all processes."
wait
