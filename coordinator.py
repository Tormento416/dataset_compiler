import os
import threading
import uuid
from typing import List, Dict, Optional
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from pathlib import Path

app = FastAPI(title="Dataset Compilation Coordinator")
task_lock = threading.Lock()

class JobSubmit(BaseModel):
    files: List[str]
    output_file: str
    dedup_columns: List[str]
    column_mapping: Optional[Dict[str, str]] = None
    compression: str = "zstd"

# In-memory queue of file tasks
pending_files = []
completed_files = []
failed_files = []

# Job config
current_job_config = None
job_active = False
job_total_files = 0

@app.post("/job")
def submit_job(job: JobSubmit):
    global current_job_config, job_active, pending_files, completed_files, failed_files, job_total_files

    if job_active and len(pending_files) > 0:
        raise HTTPException(status_code=400, detail="A job is already running.")

    current_job_config = job
    pending_files = job.files.copy()
    completed_files = []
    failed_files = []
    job_active = True
    job_total_files = len(pending_files)
    
    # ensure output dir exists
    Path(job.output_file).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    
    return {"message": f"Job started with {len(pending_files)} files."}

@app.get("/task")
def get_task():
    global job_active, pending_files, current_job_config
    with task_lock:
        if not job_active or not pending_files:
            return {"task": None, "message": "No tasks available"}
        file_path = pending_files.pop(0)
    task_id = str(uuid.uuid4())
    
    return {
        "task_id": task_id,
        "file": file_path,
        "output_file": current_job_config.output_file,
        "dedup_columns": current_job_config.dedup_columns,
        "column_mapping": current_job_config.column_mapping,
        "compression": current_job_config.compression
    }

class ProgressReport(BaseModel):
    task_id: str
    file: str
    status: str # "success" or "error"
    error_msg: Optional[str] = None

@app.post("/progress")
def report_progress(report: ProgressReport):
    global completed_files, failed_files
    if report.status == "success":
        completed_files.append(report.file)
    else:
        failed_files.append({"file": report.file, "error": report.error_msg})
    return {"status": "ok"}

@app.get("/status")
def get_status():
    global current_job_config, pending_files, completed_files, failed_files, job_active, job_total_files
    if not current_job_config:
        return {"status": "No active job."}

    return {
        "job_active": job_active,
        "total": job_total_files,
        "pending": len(pending_files),
        "completed": len(completed_files),
        "failed": len(failed_files),
        "output_file": current_job_config.output_file
    }
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
