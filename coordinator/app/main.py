from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.responses import Response
import httpx
import uuid
from sqlalchemy.orm import Session
from fastapi import Depends
from app.database import get_db, SessionLocal
from app.models import FileMetadata, StorageNode, FileReplica, FileChunk, ChunkReplica
import asyncio
from contextlib import asynccontextmanager
import hashlib

CHUNK_SIZE = 1024 * 1024

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

REPLICATION_FACTOR = 2
REPAIR_INTERVAL = 15

async def repair_replica_loop():
    while True:
        db = SessionLocal()

        try:
            active_nodes = db.query(StorageNode).filter(
                StorageNode.status == "active"
            ).all()

            if len(active_nodes) >= REPLICATION_FACTOR:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    chunks = db.query(FileChunk).all()

                    for chunk in chunks:
                        replicas = db.query(ChunkReplica).filter(
                            ChunkReplica.chunk_id == chunk.id
                        ).all()

                        valid_copies = {}
                        source_content = None
                        expected_checksum = chunk.checksum

                        for replica in replicas:
                            node = db.query(StorageNode).filter(
                                StorageNode.id == replica.node_id
                            ).first()

                            if node is None:
                                continue

                            try:
                                response = await client.get(
                                    f"{node.url.rstrip('/')}/chunks/"
                                    f"{replica.chunk_key}"
                                )

                                if response.status_code != 200:
                                    continue

                                content = response.content

                                if len(content) != chunk.size:
                                    continue

                                checksum = hashlib.sha256(content).hexdigest()

                                if (
                                    expected_checksum is not None
                                    and checksum != expected_checksum
                                ):
                                    continue

                                if expected_checksum is None:
                                    expected_checksum = checksum
                                    chunk.checksum = checksum

                                valid_copies[node.id] = replica

                                if source_content is None:
                                    source_content = content

                            except httpx.HTTPError:
                                continue

                        if source_content is None:
                            continue

                        missing = REPLICATION_FACTOR - len(valid_copies)

                        if missing <= 0:
                            continue

                        candidates = [
                            node for node in active_nodes
                            if node.id not in valid_copies
                        ]

                        for node in candidates[:missing]:
                            chunk_key = f"{chunk.file_id}_{chunk.chunk_number}"

                            try:
                                response = await client.post(
                                    f"{node.url.rstrip('/')}/chunks",
                                    data={"chunk_id": chunk_key},
                                    files={
                                        "file": (
                                            chunk_key,
                                            source_content,
                                            "application/octet-stream"
                                        )
                                    }
                                )

                                if response.status_code != 200:
                                    continue

                                existing = db.query(ChunkReplica).filter(
                                    ChunkReplica.chunk_id == chunk.id,
                                    ChunkReplica.node_id == node.id
                                ).first()

                                if existing:
                                    existing.chunk_key = chunk_key
                                else:
                                    db.add(ChunkReplica(
                                        chunk_id=chunk.id,
                                        node_id=node.id,
                                        chunk_key=chunk_key
                                    ))

                                valid_copies[node.id] = existing or True
                                db.commit()

                            except httpx.HTTPError:
                                db.rollback()
                                continue

        except Exception as exc:
            db.rollback()
            print(f"Replica repair error: {exc}")

        finally:
            db.close()

        await asyncio.sleep(REPAIR_INTERVAL)


@asynccontextmanager
async def lifespan(app):
    health_task = asyncio.create_task(health_check_loop())
    repair_task = asyncio.create_task(repair_replica_loop())

    try:
        yield
    finally:
        health_task.cancel()
        repair_task.cancel()

        await asyncio.gather(
            health_task,
            repair_task,
            return_exceptions=True
        )

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
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    file_content = await file.read()
    file_id = str(uuid.uuid4())

    nodes = db.query(StorageNode).filter(
        StorageNode.status == "active"
    ).all()

    if len(nodes) < 2:
        return {"error": "At least two active storage nodes are required"}

    stored_chunks = []
    chunk_records = []

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            for index, start in enumerate(range(0, len(file_content), CHUNK_SIZE)):
                chunk_data = file_content[start:start + CHUNK_SIZE]
                chunk_key = f"{file_id}_{index}"

                chunk = FileChunk(
                    id=str(uuid.uuid4()),
                    file_id=file_id,
                    chunk_number=index,
                    size=len(chunk_data),
                    checksum=hashlib.sha256(chunk_data).hexdigest()
                )
                db.add(chunk)
                db.flush()

                successful_replicas = 0

                for node in nodes[:2]:
                    response = await client.post(
                        f"{node.url.rstrip('/')}/chunks",
                        data={"chunk_id": chunk_key},
                        files={
                            "file": (
                                chunk_key,
                                chunk_data,
                                "application/octet-stream"
                            )
                        }
                    )

                    if response.status_code != 200:
                        raise Exception(
                            f"Chunk {index} failed on node {node.id}: "
                            f"{response.status_code} {response.text}"
                        )

                    db.add(ChunkReplica(
                        chunk_id=chunk.id,
                        node_id=node.id,
                        chunk_key=chunk_key
                    ))

                    stored_chunks.append((node.url, chunk_key))
                    successful_replicas += 1

                if successful_replicas < 2:
                    raise Exception(f"Chunk {index} was not replicated twice")

                chunk_records.append({
                    "chunk_index": index,
                    "chunk_size": len(chunk_data),
                    "replicas": 2
                })

        db.add(FileMetadata(
            id=file_id,
            filename=file.filename or "uploaded_file",
            size=len(file_content),
            node_id=nodes[0].id
        ))

        db.commit()

        return {
            "file_id": file_id,
            "filename": file.filename,
            "size": len(file_content),
            "chunk_size": CHUNK_SIZE,
            "total_chunks": len(chunk_records),
            "replication_factor": 2,
            "chunks": chunk_records
        }

    except Exception as exc:
        db.rollback()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for node_url, chunk_key in stored_chunks:
                try:
                    await client.delete(
                        f"{node_url.rstrip('/')}/chunks/{chunk_key}"
                    )
                except httpx.HTTPError:
                    pass

        return {"error": f"Upload failed: {str(exc)}"}



@app.get("/files/{file_id}")
async def download_file(file_id: str, db: Session = Depends(get_db)):
    metadata = db.query(FileMetadata).filter(
        FileMetadata.id == file_id
    ).first()

    if metadata is None:
        raise HTTPException(status_code=404, detail="File not found")

    chunks = db.query(FileChunk).filter(
        FileChunk.file_id == file_id
    ).order_by(FileChunk.chunk_number).all()

    reconstructed = bytearray()

    async with httpx.AsyncClient(timeout=60.0) as client:
        for chunk in chunks:
            replicas = db.query(ChunkReplica).filter(
                ChunkReplica.chunk_id == chunk.id
            ).all()

            chunk_content = None

            for replica in replicas:
                node = db.query(StorageNode).filter(
                    StorageNode.id == replica.node_id
                ).first()

                if node is None:
                    continue

                try:
                    response = await client.get(
                        f"{node.url.rstrip('/')}/chunks/{replica.chunk_key}"
                    )

                    if response.status_code == 200:
                        if len(response.content) == chunk.size:
                            actual_checksum = hashlib.sha256(response.content).hexdigest()
                            if chunk.checksum is None or actual_checksum == chunk.checksum:
                                chunk_content = response.content
                                break

                except httpx.HTTPError:
                    continue

            if chunk_content is None:
                raise HTTPException(
                    status_code=503,
                    detail=f"Chunk {chunk.chunk_number} is unavailable"
                )

            reconstructed.extend(chunk_content)

    if len(reconstructed) != metadata.size:
        raise HTTPException(
            status_code=500,
            detail="Reconstructed file size does not match metadata"
        )

    from urllib.parse import quote

    filename = quote(metadata.filename or "downloaded_file")

    return Response(
        content=bytes(reconstructed),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{filename}"
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