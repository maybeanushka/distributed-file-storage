from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime
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