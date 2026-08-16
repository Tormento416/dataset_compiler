import os
import sys
import queue
import threading
import asyncio
import time
from pathlib import Path
from typing import List, Dict, Optional
import json
import tkinter as tk
from tkinter import filedialog, messagebox
import customtkinter as ctk
from concurrent.futures import ProcessPoolExecutor, as_completed
from filelock import FileLock

import psutil
try:
    import pynvml
    pynvml.nvmlInit()
    HAS_NVML = True
except Exception:
    HAS_NVML = False

import aiohttp
import polars as pl
from tqdm.asyncio import tqdm

ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

class RedirectText:
    def __init__(self, log_queue):
        self.log_queue = log_queue
    def write(self, string):
        if string:
            self.log_queue.put(string)
    def flush(self):
        pass

# ==========================================
# Worker Functions (User's Code)
# ==========================================
def find_local_datasets(
    search_paths: List[str],
    extensions: List[str] = [".parquet", ".csv", ".json", ".jsonl", ".pdf"],
    recursive: bool = True
) -> List[Path]:
    found_files = set()
    for path_str in search_paths:
        path = Path(path_str).expanduser().resolve()
        if path.is_file() and path.suffix.lower() in extensions:
            found_files.add(path)
        elif path.is_dir():
            pattern = f"**/*" if recursive else "*"
            for ext in extensions:
                found_files.update(path.glob(f"{pattern}{ext}"))
    files_list = sorted([f for f in found_files if f.is_file()])
    print(f"Discovered {len(files_list)} local dataset file(s) across specified directories.")
    return files_list

async def _download_single_file(session: aiohttp.ClientSession, url: str, dest_dir: Path, semaphore: asyncio.Semaphore) -> Optional[Path]:
    try:
        clean_url = url.split("?")[0].rstrip("/")
        filename_str = clean_url.split("/")[-1]
        
        if not filename_str:
            import time
            filename_str = f"file_{int(time.time())}.bin"
            
        filename = dest_dir / filename_str
        
        if filename.is_dir():
            import time
            filename = dest_dir / f"{filename_str}_{int(time.time())}.bin"
            
        async with semaphore:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(filename, "wb") as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        f.write(chunk)
        return filename
    except Exception as e:
        print(f"[DOWNLOAD ERROR] Failed to download {url}: {e}")
        return None

async def download_datasets(urls: List[str], output_dir: str, max_concurrent: int = 8, headers: Optional[Dict[str, str]] = None) -> List[Path]:
    dest_path = Path(output_dir).expanduser().resolve()
    dest_path.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [_download_single_file(session, url, dest_path, semaphore) for url in urls]
        results = await tqdm.gather(*tasks, desc="Downloading Datasets", file=sys.stdout)
        return [r for r in results if r is not None]

def _scan_json_any(file_path: Path):
    import polars as pl
    with open(file_path, "rb") as f:
        first_byte = b""
        while True:
            b = f.read(1)
            if not b:
                break
            if not b.isspace():
                first_byte = b
                break
    if first_byte == b"[":
        return pl.read_json(file_path).lazy()
    return pl.scan_ndjson(file_path)

def _extract_pdf_text(file_path: Path):
    import pymupdf
    import polars as pl
    doc = pymupdf.open(file_path)
    try:
        text = "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()
    return pl.DataFrame({"id": [file_path.stem], "text": [text]}).lazy()

def process_single_file(file_path: Path, output_file: str, dedup_columns: List[str], column_mapping: Optional[Dict[str, str]], compression: str):
    import polars as pl
    from filelock import FileLock

    ext = file_path.suffix.lower()
    try:
        if ext == ".parquet":
            lf = pl.scan_parquet(file_path)
        elif ext == ".jsonl":
            lf = pl.scan_ndjson(file_path)
        elif ext == ".json":
            lf = _scan_json_any(file_path)
        elif ext == ".csv":
            lf = pl.scan_csv(file_path, ignore_errors=True, truncate_ragged_lines=True)
        elif ext == ".pdf":
            lf = _extract_pdf_text(file_path)
        else:
            return f"Skipped unsupported file: {file_path}"
            
        if column_mapping:
            existing_cols = lf.collect_schema().names()
            rename_dict = {k: v for k, v in column_mapping.items() if k in existing_cols}
            if rename_dict:
                lf = lf.rename(rename_dict)

        if dedup_columns:
            existing_cols = lf.collect_schema().names()
            valid_dedup_cols = [c for c in dedup_columns if c in existing_cols]
            if valid_dedup_cols:
                lf = lf.unique(subset=valid_dedup_cols, keep="first")

        df_new = lf.collect()
        
        target_path = Path(output_file).expanduser().resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = str(target_path) + ".lock"
        
        with FileLock(lock_path, timeout=300):
            if target_path.exists():
                df_existing = pl.read_parquet(target_path)
                df_combined = pl.concat([df_existing, df_new], how="diagonal")
                
                if dedup_columns:
                    valid_dedup_cols = [c for c in dedup_columns if c in df_combined.columns]
                    if valid_dedup_cols:
                        df_combined = df_combined.unique(subset=valid_dedup_cols, keep="first")
                        
                df_combined.write_parquet(target_path, compression=compression)
            else:
                df_new.write_parquet(target_path, compression=compression)
                
        return f"Successfully processed {file_path.name}"
    except Exception as e:
        return f"Error processing {file_path.name}: {e}"

# ==========================================
# GUI Application
# ==========================================
class DatasetCompilerApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Dataset Compiler")
        self.geometry("900x700")
        
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.tabview = ctk.CTkTabview(self)
        self.tabview.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")
        
        self.tab1 = self.tabview.add("Configuration")
        self.tab2 = self.tabview.add("Dashboard & Logs")

        # Configuration Tab
        self.tab1.grid_columnconfigure(1, weight=1)
        
        # Remote URLs
        ctk.CTkLabel(self.tab1, text="Remote URLs (one per line):").grid(row=0, column=0, padx=10, pady=5, sticky="nw")
        self.urls_text = ctk.CTkTextbox(self.tab1, height=100)
        self.urls_text.grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        self.urls_text.insert("1.0", "https://example.com/data_part1.jsonl\nhttps://example.com/data_part2.csv")
        
        # Local Search Paths
        ctk.CTkLabel(self.tab1, text="Local Search Paths:").grid(row=1, column=0, padx=10, pady=5, sticky="nw")
        self.local_paths_text = ctk.CTkTextbox(self.tab1, height=80)
        self.local_paths_text.grid(row=1, column=1, padx=10, pady=5, sticky="ew")
        self.local_paths_text.insert("1.0", "")
        self.add_path_btn = ctk.CTkButton(self.tab1, text="Add Folder", command=self.add_local_path)
        self.add_path_btn.grid(row=1, column=2, padx=10, pady=5, sticky="nw")
        
        self.add_file_btn = ctk.CTkButton(self.tab1, text="Add File(s)", command=self.add_local_file)
        self.add_file_btn.grid(row=1, column=2, padx=10, pady=40, sticky="nw")

        # Column Mapping
        ctk.CTkLabel(self.tab1, text="Column Mapping (JSON format):").grid(row=2, column=0, padx=10, pady=5, sticky="nw")
        self.col_map_text = ctk.CTkTextbox(self.tab1, height=120)
        self.col_map_text.grid(row=2, column=1, padx=10, pady=5, sticky="ew")
        default_map = '{\n    "raw_text": "text",\n    "content": "text",\n    "document_id": "id",\n    "guid": "id"\n}'
        self.col_map_text.insert("1.0", default_map)

        # Deduplication Columns
        ctk.CTkLabel(self.tab1, text="Deduplication Columns (comma-separated):").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.dedup_var = tk.StringVar(value="text")
        ctk.CTkEntry(self.tab1, textvariable=self.dedup_var).grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        # Output File
        ctk.CTkLabel(self.tab1, text="Output Parquet File:").grid(row=4, column=0, padx=10, pady=5, sticky="w")
        default_storage = Path.home() / "DatasetCompiler"
        self.output_var = tk.StringVar(value=str(default_storage / "consolidated_output" / "train_data.parquet"))
        ctk.CTkEntry(self.tab1, textvariable=self.output_var).grid(row=4, column=1, padx=10, pady=5, sticky="ew")
        ctk.CTkButton(self.tab1, text="Browse", command=self.browse_output).grid(row=4, column=2, padx=10, pady=5)

        # Temp Download Dir
        ctk.CTkLabel(self.tab1, text="Temp Download Dir:").grid(row=5, column=0, padx=10, pady=5, sticky="w")
        self.temp_dir_var = tk.StringVar(value=str(default_storage / "raw_data"))
        ctk.CTkEntry(self.tab1, textvariable=self.temp_dir_var).grid(row=5, column=1, padx=10, pady=5, sticky="ew")
        ctk.CTkButton(self.tab1, text="Change Storage Folder", command=self.change_storage_dir).grid(row=5, column=2, padx=10, pady=5)

        # API Headers
        ctk.CTkLabel(self.tab1, text="API Headers (JSON):").grid(row=6, column=0, padx=10, pady=5, sticky="nw")
        self.api_headers_text = ctk.CTkTextbox(self.tab1, height=60)
        self.api_headers_text.grid(row=6, column=1, padx=10, pady=5, sticky="ew")
        self.api_headers_text.insert("1.0", "{\n    \"Authorization\": \"Bearer hf_YOUR_TOKEN\"\n}")

        self.start_btn = ctk.CTkButton(self.tab1, text="Start Processing", command=self.start_processing)
        self.start_btn.grid(row=7, column=1, pady=20)

        # Dashboard & Logs Tab
        self.tab2.grid_columnconfigure(0, weight=1)
        self.tab2.grid_columnconfigure(1, weight=1)
        self.tab2.grid_rowconfigure(5, weight=1)

        ctk.CTkLabel(self.tab2, text="Progress:", font=("Arial", 16, "bold")).grid(row=0, column=0, pady=(10,5), sticky="e")
        self.progress_bar = ctk.CTkProgressBar(self.tab2, width=400)
        self.progress_bar.grid(row=0, column=1, pady=(10,5), padx=20, sticky="w")
        self.progress_bar.set(0)

        ctk.CTkLabel(self.tab2, text="Completion:", font=("Arial", 14, "bold")).grid(row=1, column=0, pady=5, sticky="e")
        self.pct_label = ctk.CTkLabel(self.tab2, text="0%")
        self.pct_label.grid(row=1, column=1, pady=5, sticky="w", padx=20)

        ctk.CTkLabel(self.tab2, text="ETA:", font=("Arial", 14, "bold")).grid(row=2, column=0, pady=5, sticky="e")
        self.eta_label = ctk.CTkLabel(self.tab2, text="N/A")
        self.eta_label.grid(row=2, column=1, pady=5, sticky="w", padx=20)

        ctk.CTkLabel(self.tab2, text="System RAM:", font=("Arial", 14, "bold")).grid(row=3, column=0, pady=5, sticky="e")
        self.ram_label = ctk.CTkLabel(self.tab2, text="0 MB / 0 MB")
        self.ram_label.grid(row=3, column=1, pady=5, sticky="w", padx=20)

        ctk.CTkLabel(self.tab2, text="GPU VRAM:", font=("Arial", 14, "bold")).grid(row=4, column=0, pady=5, sticky="e")
        self.vram_label = ctk.CTkLabel(self.tab2, text="N/A")
        self.vram_label.grid(row=4, column=1, pady=5, sticky="w", padx=20)

        self.log_textbox = ctk.CTkTextbox(self.tab2)
        self.log_textbox.grid(row=5, column=0, columnspan=2, padx=10, pady=10, sticky="nsew")

        self.log_queue = queue.Queue()
        self.redirector = RedirectText(self.log_queue)

        self.monitor_active = True
        threading.Thread(target=self.monitor_hardware, daemon=True).start()
        
        self.after(100, self.process_log_queue)

        # Auto-load previous configuration
        self.load_last_run()
        if not self.storage_dir_configured:
            self.change_storage_dir()

    def get_config_path(self):
        return Path.home() / ".dataset_compiler_config.json"

    def prompt_storage_dir(self):
        chosen = filedialog.askdirectory(
            title="Choose a folder to store compiled datasets and downloaded files"
        )
        if not chosen:
            chosen = str(Path.home() / "DatasetCompiler")
        storage_dir = Path(chosen).expanduser().resolve()
        storage_dir.mkdir(parents=True, exist_ok=True)
        return storage_dir

    def change_storage_dir(self):
        storage_dir = self.prompt_storage_dir()
        self.output_var.set(str(storage_dir / "consolidated_output" / "train_data.parquet"))
        self.temp_dir_var.set(str(storage_dir / "raw_data"))
        self.storage_dir_configured = True
        self.save_last_run()

    def load_last_run(self):
        config_path = self.get_config_path()
        self.storage_dir_configured = False
        if config_path.exists():
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
                self.urls_text.delete("1.0", "end")
                self.urls_text.insert("1.0", config.get("urls", "https://example.com/data_part1.jsonl\nhttps://example.com/data_part2.csv"))

                self.local_paths_text.delete("1.0", "end")
                self.local_paths_text.insert("1.0", config.get("local_paths", ""))

                self.col_map_text.delete("1.0", "end")
                self.col_map_text.insert("1.0", config.get("col_map", "{\n    \"raw_text\": \"text\",\n    \"content\": \"text\",\n    \"document_id\": \"id\",\n    \"guid\": \"id\"\n}"))

                self.dedup_var.set(config.get("dedup_cols", "text"))

                if "storage_dir" in config:
                    self.output_var.set(config.get("output_file", self.output_var.get()))
                    self.temp_dir_var.set(config.get("temp_dir", self.temp_dir_var.get()))
                    self.storage_dir_configured = True

                self.api_headers_text.delete("1.0", "end")
                self.api_headers_text.insert("1.0", config.get("api_headers", "{\n    \"Authorization\": \"Bearer hf_YOUR_TOKEN\"\n}"))
            except Exception as e:
                print(f"Error loading last run: {e}")

    def save_last_run(self):
        config = {
            "urls": self.urls_text.get("1.0", "end-1c"),
            "local_paths": self.local_paths_text.get("1.0", "end-1c"),
            "col_map": self.col_map_text.get("1.0", "end-1c"),
            "dedup_cols": self.dedup_var.get(),
            "output_file": self.output_var.get(),
            "temp_dir": self.temp_dir_var.get(),
            "storage_dir": True,
            "api_headers": self.api_headers_text.get("1.0", "end-1c")
        }
        try:
            with open(self.get_config_path(), "w") as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            print(f"Error saving last run: {e}")

    def monitor_hardware(self):
        wmi_conn = None
        if not HAS_NVML and sys.platform == "win32":
            try:
                import wmi
                wmi_conn = wmi.WMI()
            except Exception:
                pass
                
        while self.monitor_active:
            try:
                mem = psutil.virtual_memory()
                ram_used = mem.used / (1024**3)
                ram_tot = mem.total / (1024**3)
                if hasattr(self, 'ram_label') and self.ram_label.winfo_exists():
                    self.ram_label.configure(text=f"{ram_used:.1f} GB / {ram_tot:.1f} GB")
                
                if hasattr(self, 'vram_label') and self.vram_label.winfo_exists():
                    vram_text = "N/A"
                    if HAS_NVML:
                        try:
                            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                            info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                            vram_used = info.used / (1024**3)
                            vram_tot = info.total / (1024**3)
                            vram_text = f"{vram_used:.1f} GB / {vram_tot:.1f} GB"
                        except Exception:
                            vram_text = "NVIDIA GPU error"
                    elif wmi_conn:
                        try:
                            gpu_info = ""
                            for gpu in wmi_conn.Win32_VideoController():
                                name = gpu.Name
                                ram = gpu.AdapterRAM
                                if ram:
                                    ram_gb = abs(int(ram)) / (1024**3)
                                    gpu_info += f"{name} ({ram_gb:.1f} GB Total)  "
                            if gpu_info:
                                vram_text = gpu_info.strip()
                            else:
                                vram_text = "WMI GPU info not found"
                        except Exception:
                            vram_text = "Error reading WMI VRAM"
                    else:
                        vram_text = "GPU monitor not available"
                        
                    self.vram_label.configure(text=vram_text)
            except Exception:
                pass
            time.sleep(1)

    def add_local_path(self):
        folder = filedialog.askdirectory()
        if folder:
            current = self.local_paths_text.get("1.0", "end-1c")
            if current.strip():
                self.local_paths_text.insert("end", f"\n{folder}")
            else:
                self.local_paths_text.insert("end", folder)

    def add_local_file(self):
        files = filedialog.askopenfilenames(filetypes=[("Dataset Files", "*.csv *.parquet *.json *.jsonl *.pdf"), ("All Files", "*.*")])
        if files:
            for file in files:
                current = self.local_paths_text.get("1.0", "end-1c")
                if current.strip():
                    self.local_paths_text.insert("end", f"\n{file}")
                else:
                    self.local_paths_text.insert("end", file)

    def browse_output(self):
        file = filedialog.asksaveasfilename(defaultextension=".parquet", filetypes=[("Parquet Files", "*.parquet")])
        if file:
            self.output_var.set(file)

    def process_log_queue(self):
        while not self.log_queue.empty():
            msg = self.log_queue.get()
            self.log_textbox.insert("end", msg)
            self.log_textbox.see("end")
        self.after(50, self.process_log_queue)

    def start_processing(self):
        urls = [u.strip() for u in self.urls_text.get("1.0", "end-1c").splitlines() if u.strip()]
        local_paths = [p.strip() for p in self.local_paths_text.get("1.0", "end-1c").splitlines() if p.strip()]
        
        try:
            col_map_str = self.col_map_text.get("1.0", "end-1c").strip()
            column_map = json.loads(col_map_str) if col_map_str else None
        except json.JSONDecodeError:
            messagebox.showerror("Error", "Invalid JSON format in Column Mapping.")
            return

        try:
            headers_str = self.api_headers_text.get("1.0", "end-1c").strip()
            headers_dict = json.loads(headers_str) if headers_str else None
        except json.JSONDecodeError:
            messagebox.showerror("Error", "Invalid JSON format in API Headers.")
            return

        dedup_cols = [c.strip() for c in self.dedup_var.get().split(",") if c.strip()]
        output_file = self.output_var.get()
        download_dir = self.temp_dir_var.get()

        if not urls and not local_paths:
            messagebox.showwarning("Warning", "Please provide at least one URL or Local Search Path.")
            return

        self.save_last_run()
        
        self.start_btn.configure(state="disabled")
        self.tabview.set("Dashboard & Logs")
        
        # Save old stdout/stderr
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.redirector
        sys.stderr = self.redirector

        print("Starting pipeline...")
        threading.Thread(
            target=self.run_pipeline, 
            args=(urls, local_paths, download_dir, output_file, column_map, dedup_cols, headers_dict), 
            daemon=True
        ).start()

    def run_pipeline(self, urls, local_paths, download_dir, output_file, column_map, dedup_cols, headers):
        try:
            downloaded_files = []
            if urls:
                print(f"Starting async download for {len(urls)} urls...")
                downloaded_files = asyncio.run(download_datasets(urls, download_dir, max_concurrent=4, headers=headers))
            
            local_files = []
            if local_paths:
                print("Scanning local paths...")
                local_files = find_local_datasets(search_paths=local_paths, recursive=True)

            all_files = list(set(downloaded_files + local_files))
            if not all_files:
                print("No files found or downloaded to process.")
                return

            print(f"Total files to process: {len(all_files)}")
            
            # Delete existing output file if starting fresh
            target_out = Path(output_file).expanduser().resolve()
            if target_out.exists():
                target_out.unlink()
                
            num_workers = min(4, len(all_files)) if all_files else 1
            print(f"Starting {num_workers} concurrent writers...")
            
            t0 = time.time()
            completed = 0
            total_files = len(all_files)
            
            # Reset UI
            self.after(0, self.progress_bar.set, 0)
            self.after(0, lambda: self.pct_label.configure(text="0%"))
            self.after(0, lambda: self.eta_label.configure(text="Calculating..."))
            
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = {
                    executor.submit(process_single_file, f, output_file, dedup_cols, column_map, "zstd"): f 
                    for f in all_files
                }
                
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        print(res)
                    except Exception as exc:
                        print(f"File generated an exception: {exc}")
                        
                    completed += 1
                    pct = completed / total_files
                    elapsed = time.time() - t0
                    time_per_file = elapsed / completed
                    eta_sec = time_per_file * (total_files - completed)
                    
                    self.after(0, self.progress_bar.set, pct)
                    self.after(0, lambda t=f"{int(pct*100)}%": self.pct_label.configure(text=t))
                    self.after(0, lambda t=f"{eta_sec:.1f}s": self.eta_label.configure(text=t))

            print("Pipeline finished successfully!")
            
            try:
                out_path = Path(output_file).expanduser().resolve()
                if out_path.exists():
                    size_mb = out_path.stat().st_size / (1024 * 1024)
                    row_count = pl.scan_parquet(out_path).select(pl.len()).collect().item()
                    msg = f"Dataset compiled successfully!\nTotal Records: {row_count:,}\nTotal Size: {size_mb:.2f} MB"
                    print(f"\n[REPORT]\n{msg}")
                    self.after(0, lambda: messagebox.showinfo("Success", msg))
            except Exception as e:
                print(f"Failed to generate report: {e}")
        except Exception as e:
            print(f"\n[ERROR] Pipeline failed: {e}")
        finally:
            self.start_btn.configure(state="normal")
            sys.stdout = self.old_stdout
            sys.stderr = self.old_stderr

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    app = DatasetCompilerApp()
    app.mainloop()
