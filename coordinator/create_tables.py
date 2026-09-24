from app.database import engine, Base
from app.models import FileMetadata, StorageNode, FileReplica

Base.metadata.create_all(bind=engine)

print("Tables created successfully")