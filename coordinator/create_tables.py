from app.database import engine, Base
from app.models import FileMetadata

Base.metadata.create_all(bind=engine)

print("Tables created successfully")