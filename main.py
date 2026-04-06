import subprocess
import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Form ADV Lookup")

FETCH_ADV = Path(__file__).parent / "fetch_adv.py"


class LookupRequest(BaseModel):
    name: Optional[str] = None
    crd: Optional[str] = None


STATIC = Path(__file__).parent / "static"


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

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    if result.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail=result.stderr.strip() or "fetch_adv.py failed with no output",
        )

    return {
        "output": result.stdout.strip(),
        "log": result.stderr.strip(),
    }
