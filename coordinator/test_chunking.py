
import hashlib
import httpx

BASE_URL = "http://127.0.0.1:8000"

content = bytes(i % 256 for i in range(3 * 1024 * 1024 + 123))

with httpx.Client(timeout=120.0) as client:
    upload = client.post(
        f"{BASE_URL}/files",
        files={
            "file": (
                "chunk_test.bin",
                content,
                "application/octet-stream"
            )
        }
    )

    print("Upload status:", upload.status_code)
    print("Upload response:", upload.text)
    upload.raise_for_status()

    result = upload.json()
    file_id = result["file_id"]

    print("File ID:", file_id)
    print("Expected chunks:", 4)
    print("Actual chunks:", result["total_chunks"])
    print("Replication factor:", result["replication_factor"])

    download = client.get(f"{BASE_URL}/files/{file_id}")
    print("Download status:", download.status_code)
    download.raise_for_status()

    downloaded = download.content

    print("Original size:", len(content))
    print("Downloaded size:", len(downloaded))
    print("Original SHA-256:", hashlib.sha256(content).hexdigest())
    print("Downloaded SHA-256:", hashlib.sha256(downloaded).hexdigest())
    print("Exact match:", content == downloaded)

    if content != downloaded:
        raise AssertionError("Downloaded file differs from original")

    print("CHUNKING TEST PASSED")
