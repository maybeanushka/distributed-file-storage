from fastapi import FastAPI

app = FastAPI(title="Distributed Storage Coordinator")


@app.get("/")
def root():
    return {"message": "Coordinator is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}