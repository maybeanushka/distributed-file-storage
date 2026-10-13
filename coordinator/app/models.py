from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from app.database import Base


class FileMetadata(Base):
    __tablename__ = "files"

    id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    node_id = Column(String, nullable=False)
    uploaded_at = Column(DateTime, default=datetime.utcnow)

class StorageNode(Base):
    __tablename__ = "storage_nodes"

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    status = Column(String, nullable=False, default="active")

class FileReplica(Base):
    __tablename__ = "file_replicas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String, nullable=False)
    node_id = Column(String, nullable=False)

class FileChunk(Base):
    __tablename__ = "file_chunks"

    id = Column(String, primary_key=True, index=True)
    file_id = Column(String, ForeignKey("files.id"), nullable=False, index=True)
    chunk_number = Column(Integer, nullable=False)
    size = Column(Integer, nullable=False)
    checksum = Column(String, nullable=True)

    replicas = relationship(
        "ChunkReplica",
        back_populates="chunk",
        cascade="all, delete-orphan"
    )

class ChunkReplica(Base):
    __tablename__ = "chunk_replicas"

    id = Column(Integer, primary_key=True, index=True)
    chunk_id = Column(String, ForeignKey("file_chunks.id"), nullable=False)
    node_id = Column(String, ForeignKey("storage_nodes.id"), nullable=False)
    chunk_key = Column(String, nullable=True)

    chunk = relationship("FileChunk", back_populates="replicas")
