from fastapi import FastAPI, UploadFile, File, Depends
from fastapi.responses import Response
import httpx
import uuid
from sqlalchemy.orm import Session
from fastapi import Depends
from app.database import get_db, SessionLocal
from app.models import FileMetadata, StorageNode, FileReplica
import asyncio
from contextlib import asynccontextmanager

async def health_check_loop():
    while True:
        try:
            db = SessionLocal()

            nodes = db.query(StorageNode).all()

            async with httpx.AsyncClient(timeout=2.0) as client:
                for node in nodes:
                    try:
                        response = await client.get(
                            f"{node.url}/health"
                        )

                        if response.status_code == 200:
                            node.status = "active"
                        else:
                            node.status = "inactive"

                    except Exception:
                        node.status = "inactive"

            db.commit()
            db.close()

        except Exception as e:
            print("Health check error:", e)

        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(health_check_loop())

    yield

    task.cancel()

app = FastAPI(
    title="Distributed Storage Coordinator",
    lifespan=lifespan
)

@app.get("/")
def root():
    return {"message": "Coordinator is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/files")
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    nodes = db.query(StorageNode).filter(
        StorageNode.status == "active"
    ).all()

    if len(nodes) < 2:
        return {"error": "At least 2 active storage nodes required"}

    nodes = sorted(
        nodes,
        key=lambda n: db.query(FileMetadata).filter(
            FileMetadata.node_id == n.id
        ).count()
    )

    selected_nodes = nodes[:2]

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

    replicas = []

    async with httpx.AsyncClient(timeout=5.0) as client:
        for node in selected_nodes:
            try:
                response = await client.post(
                    f"{node.url}/files",
                    files=files,
                    data=data
                )

                if response.status_code != 200:
                    raise Exception(
                        f"Upload failed on {node.id}"
                    )

                replicas.append(node)

            except Exception:
                for uploaded_node in replicas:
                    try:
                        await client.delete(
                            f"{uploaded_node.url}/files/{file_id}"
                        )
                    except Exception:
                        pass

                return {
                    "error": f"Replication failed on {node.id}"
                }

    file_metadata = FileMetadata(
        id=file_id,
        filename=file.filename,
        size=len(file_content),
        node_id=selected_nodes[0].id
    )

    db.add(file_metadata)

    for node in replicas:
        replica = FileReplica(
            file_id=file_id,
            node_id=node.id
        )

        db.add(replica)

    db.commit()

    return {
        "file_id": file_id,
        "filename": file.filename,
        "size": len(file_content),
        "replicas": [
            node.id for node in replicas
        ]
    }

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

    replicas = db.query(FileReplica).filter(
        FileReplica.file_id == file_id
    ).all()

    if not replicas:
        return {"error": "No replicas found"}

    replica_nodes = []

    for replica in replicas:
        node = db.query(StorageNode).filter(
            StorageNode.id == replica.node_id,
            StorageNode.status == "active"
        ).first()

        if node:
            replica_nodes.append(node)

    if not replica_nodes:
        return {"error": "No active replicas available"}

    async with httpx.AsyncClient(timeout=5.0) as client:

        for node in replica_nodes:
            try:
                response = await client.get(
                    f"{node.url}/files/{file_id}"
                )

                if response.status_code == 200:
                    return Response(
                        content=response.content,
                        media_type="application/octet-stream",
                        headers={
                            "Content-Disposition":
                            f'attachment; filename="{file_metadata.filename}"'
                        }
                    )

            except Exception:
                continue

    return {"error": "File could not be retrieved from any replica"}

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

@app.get("/nodes/health")
async def check_nodes_health(
    db: Session = Depends(get_db)
):
    nodes = db.query(StorageNode).all()

    results = []

    async with httpx.AsyncClient(timeout=2.0) as client:
        for node in nodes:
            try:
                response = await client.get(
                    f"{node.url}/health"
                )

                if response.status_code == 200:
                    node.status = "active"
                    results.append({
                        "node": node.id,
                        "status": "active"
                    })
                else:
                    node.status = "inactive"
                    results.append({
                        "node": node.id,
                        "status": "inactive"
                    })

            except Exception:
                node.status = "inactive"

                results.append({
                    "node": node.id,
                    "status": "inactive"
                })

    db.commit()

    return results