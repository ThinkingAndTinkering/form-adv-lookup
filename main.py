import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Form ADV Lookup")

FETCH_ADV = Path(__file__).parent / "fetch_adv.py"
STATIC = Path(__file__).parent / "static"

# In-memory job store (fine for single-instance free tier)
jobs: dict = {}
executor = ThreadPoolExecutor(max_workers=4)


class LookupRequest(BaseModel):
    name: Optional[str] = None
    crd: Optional[str] = None


def run_lookup(job_id: str, cmd: list):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            jobs[job_id] = {
                "status": "error",
                "detail": result.stderr.strip() or "fetch_adv.py failed",
            }
        else:
            jobs[job_id] = {"status": "done", "output": result.stdout.strip()}
    except subprocess.TimeoutExpired:
        jobs[job_id] = {"status": "error", "detail": "Timed out after 3 minutes"}
    except Exception as e:
        jobs[job_id] = {"status": "error", "detail": str(e)}


@app.get("/", response_class=HTMLResponse)
def index():
    return (STATIC / "index.html").read_text()


@app.post("/lookup")
def lookup(req: LookupRequest):
    if not req.name and not req.crd:
        raise HTTPException(status_code=400, detail="Provide 'name' or 'crd'")

    cmd = [sys.executable, str(FETCH_ADV)]
    if req.crd:
        cmd += ["--crd", req.crd]
    else:
        cmd += ["--name", req.name]

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending"}
    executor.submit(run_lookup, job_id, cmd)

    return {"job_id": job_id}


@app.get("/status/{job_id}")
def status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
