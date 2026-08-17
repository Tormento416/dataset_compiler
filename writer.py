import time
import requests
import polars as pl
from pathlib import Path
import uuid
from filelock import FileLock

COORDINATOR_URL = "http://127.0.0.1:8000"

def _scan_json_any(file_path: Path):
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
    doc = pymupdf.open(file_path)
    try:
        text = "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()
    return pl.DataFrame({"id": [file_path.stem], "text": [text]}).lazy()

def _drop_empty_text(df):
    """Drops rows with a null or whitespace-only 'text' value -- these are unusable
    as SLM/LLM training examples and would otherwise pass through silently."""
    if "text" not in df.columns:
        return df
    return df.filter(pl.col("text").is_not_null() & (pl.col("text").str.strip_chars() != ""))

def process_file(task):
    file_path_str = task["file"]
    output_file = task["output_file"]
    dedup_columns = task["dedup_columns"]
    column_mapping = task["column_mapping"]
    compression = task["compression"]
    task_id = task["task_id"]
    
    file = Path(file_path_str).expanduser().resolve()
    ext = file.suffix.lower()
    
    try:
        if ext == ".parquet":
            lf = pl.scan_parquet(file)
        elif ext == ".jsonl":
            lf = pl.scan_ndjson(file)
        elif ext == ".json":
            lf = _scan_json_any(file)
        elif ext == ".csv":
            lf = pl.scan_csv(file, ignore_errors=True, truncate_ragged_lines=True)
        elif ext == ".pdf":
            lf = _extract_pdf_text(file)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        if column_mapping:
            existing_cols = lf.collect_schema().names()
            rename_dict = {k: v for k, v in column_mapping.items() if k in existing_cols}
            if rename_dict:
                lf = lf.rename(rename_dict)

        # local deduplication before locking the global file to save memory
        if dedup_columns:
            existing_cols = lf.collect_schema().names()
            valid_dedup_cols = [c for c in dedup_columns if c in existing_cols]
            if valid_dedup_cols:
                lf = lf.unique(subset=valid_dedup_cols, keep="first")

        df_new = lf.collect()

        target_path = Path(output_file).expanduser().resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        lock_path = str(target_path) + ".lock"
        
        print(f"Waiting for global file lock on {target_path.name}...")
        # Use FileLock to ensure writers do not corrupt the single dataset file
        with FileLock(lock_path, timeout=300):
            if target_path.exists():
                print(f"Lock acquired. Merging and deduplicating with existing dataset...")
                df_existing = pl.read_parquet(target_path)
                
                # Align columns diagonally
                df_combined = pl.concat([df_existing, df_new], how="diagonal")
                
                # Global deduplication across all writers' data
                if dedup_columns:
                    valid_dedup_cols = [c for c in dedup_columns if c in df_combined.columns]
                    if valid_dedup_cols:
                        df_combined = df_combined.unique(subset=valid_dedup_cols, keep="first")

                df_combined = _drop_empty_text(df_combined)
                df_combined.write_parquet(target_path, compression=compression)
            else:
                print(f"Lock acquired. Creating new dataset...")
                df_new = _drop_empty_text(df_new)
                df_new.write_parquet(target_path, compression=compression)
                
        return True, None
    except Exception as e:
        return False, str(e)

def main():
    worker_id = str(uuid.uuid4())[:8]
    print(f"[{worker_id}] Writer Started. Polling {COORDINATOR_URL} for tasks...")
    
    while True:
        try:
            resp = requests.get(f"{COORDINATOR_URL}/task")
            if resp.status_code == 200:
                data = resp.json()
                if data.get("task") is None and data.get("message") == "No tasks available":
                    time.sleep(2)
                    continue
                
                print(f"[{worker_id}] Processing {data['file']} ...")
                success, error = process_file(data)
                
                status_payload = {
                    "task_id": data["task_id"],
                    "file": data["file"],
                    "status": "success" if success else "error",
                    "error_msg": error
                }
                requests.post(f"{COORDINATOR_URL}/progress", json=status_payload)
                print(f"[{worker_id}] Done {data['file']}: {'SUCCESS' if success else f'ERROR: {error}'}")
            else:
                time.sleep(2)
        except requests.exceptions.ConnectionError:
            time.sleep(2)
        except Exception as e:
            print(f"[{worker_id}] Unexpected error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
