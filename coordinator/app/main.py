from fastapi import FastAPI, UploadFile, File
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