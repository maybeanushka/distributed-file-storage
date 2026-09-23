from fastapi import FastAPI, UploadFile, File, Depends
from fastapi.responses import Response
import httpx
import uuid
from sqlalchemy.orm import Session
from fastapi import Depends
from app.database import get_db
from app.models import FileMetadata

app = FastAPI(title="Distributed Storage Coordinator")

STORAGE_NODE_URL = "http://127.0.0.1:9001"


@app.get("/")
def root():
    return {"message": "Coordinator is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/nodes/health")
async def storage_node_health():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{STORAGE_NODE_URL}/health")

    return {
        "node": STORAGE_NODE_URL,
        "status": response.json()
    }


@app.post("/files")
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    file_id = str(uuid.uuid4())

    file_content = await file.read()

    files = {
        "file": (
            file.filename,
            file_content,
            file.content_type
        )
    }

    data = {
        "file_id": file_id
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{STORAGE_NODE_URL}/files",
            files=files,
            data=data
        )

    response_data = response.json()

    file_metadata = FileMetadata(
        id=file_id,
        filename=file.filename,
        size=response_data["size"],
        node_id=STORAGE_NODE_URL
    )

    db.add(file_metadata)
    db.commit()

    return response_data

@app.get("/files/{file_id}")
async def download_file(
    file_id: str,
    db: Session = Depends(get_db)
):
    file_metadata = db.query(FileMetadata).filter(
        FileMetadata.id == file_id
    ).first()

    if not file_metadata:
        return {"error": "File not found"}

    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{file_metadata.node_id}/files/{file_id}"
        )

    if response.status_code != 200:
        return {"error": "File could not be retrieved"}

    return Response(
        content=response.content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{file_metadata.filename}"'
        }
    )

@app.get("/files")
def list_files(db: Session = Depends(get_db)):
    files = db.query(FileMetadata).all()

    return [
        {
            "file_id": file.id,
            "filename": file.filename,
            "size": file.size,
            "node_id": file.node_id,
        }
        for file in files
    ]