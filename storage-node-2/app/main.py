from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import shutil

app = FastAPI(title="Distributed Storage Node 2")

STORAGE_DIR = Path("data")
STORAGE_DIR.mkdir(exist_ok=True)


@app.get("/")
def root():
    return {"message": "Storage node is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/files")
async def upload_file(
    file: UploadFile = File(...),
    file_id: str = Form(...)
):
    file_path = STORAGE_DIR / file_id

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "file_id": file_id,
        "filename": file.filename,
        "size": file_path.stat().st_size
    }

@app.get("/files/{file_id}")
def download_file(file_id: str):
    file_path = STORAGE_DIR / file_id

    if not file_path.exists():
        return {"error": "File not found"}

    return FileResponse(file_path)

@app.delete("/files/{file_id}")
def delete_file(file_id: str):
    file_path = STORAGE_DIR / file_id

    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail="File not found"
        )

    file_path.unlink()

    return {
        "file_id": file_id,
        "status": "deleted"
    }