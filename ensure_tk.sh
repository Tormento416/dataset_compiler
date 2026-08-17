#!/usr/bin/env bash
# Sourced by build.sh and run.sh. Installs the OS-level Tk package if `import tkinter`
# fails — tkinter isn't a pip package, so this is the one dependency `pip install
# -r requirements.txt` can never satisfy on its own.
ensure_tk() {
    if python3 -c "import tkinter" 2>/dev/null; then
        return 0
    fi

    echo "tkinter not found (required for the GUI) — installing it via the system package manager..."
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update && sudo apt-get install -y python3-tk
    elif command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y python3-tkinter
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y python3-tkinter
    elif command -v pacman >/dev/null 2>&1; then
        sudo pacman -Sy --noconfirm tk
    elif command -v zypper >/dev/null 2>&1; then
        sudo zypper install -y python3-tk
    elif command -v brew >/dev/null 2>&1; then
        pyver=$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
        brew install "python-tk@${pyver}" || brew install python-tk
    else
        echo "Could not detect a supported package manager (apt/dnf/yum/pacman/zypper/brew)." >&2
        echo "Please install tkinter manually, e.g. 'sudo apt install python3-tk', then re-run this script." >&2
        exit 1
    fi

    if ! python3 -c "import tkinter" 2>/dev/null; then
        echo "tkinter install failed. Please install it manually and re-run this script." >&2
        exit 1
    fi
}
