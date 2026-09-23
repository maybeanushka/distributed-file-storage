from app.database import SessionLocal
from app.models import StorageNode


db = SessionLocal()

nodes = [
    StorageNode(
        id="node-1",
        url="http://127.0.0.1:9001",
        status="active"
    ),
    StorageNode(
        id="node-2",
        url="http://127.0.0.1:9002",
        status="active"
    )
]

db.add_all(nodes)
db.commit()
db.close()

print("Storage nodes added successfully")