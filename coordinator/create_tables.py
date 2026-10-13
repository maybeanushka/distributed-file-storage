from app.database import engine, Base
from app.models import FileMetadata, StorageNode, FileReplica, FileChunk, ChunkReplica

Base.metadata.create_all(bind=engine)

print("Tables created successfully")