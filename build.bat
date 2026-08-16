@echo off
echo Building Dataset Downloader GUI...
pyinstaller --noconfirm --onedir --windowed --name "DatasetDownloader" "downloader_gui.py"
echo Build complete! Executable is in the 'dist' folder.
pause
