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
    