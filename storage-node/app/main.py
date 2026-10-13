from pathlib import Path
import re
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, Response
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

CHUNK_DIR = Path(__file__).resolve().parent / "chunks"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)


@app.post("/chunks")
async def store_chunk(chunk_id: str = Form(...), file: UploadFile = File(...)):
    if not re.fullmatch(r"[a-fA-F0-9_-]+", chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk ID")

    content = await file.read()
    (CHUNK_DIR / chunk_id).write_bytes(content)

    return {"chunk_id": chunk_id, "size": len(content)}


@app.get("/chunks/{chunk_id}")
def get_chunk(chunk_id: str):
    if not re.fullmatch(r"[a-fA-F0-9_-]+", chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk ID")

    path = CHUNK_DIR / chunk_id

    if not path.exists():
        raise HTTPException(status_code=404, detail="Chunk not found")

    return Response(
        content=path.read_bytes(),
        media_type="application/octet-stream"
    )


@app.delete("/chunks/{chunk_id}")
def delete_chunk(chunk_id: str):
    if not re.fullmatch(r"[a-fA-F0-9_-]+", chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk ID")

    path = CHUNK_DIR / chunk_id

    if path.exists():
        path.unlink()

    return {"message": "Chunk deleted", "chunk_id": chunk_id}
