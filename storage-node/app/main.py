from fastapi import FastAPI, UploadFile, File, Form
from pathlib import Path
import shutil

app = FastAPI(title="Distributed Storage Node")

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