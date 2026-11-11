from fastapi import FastAPI

app = FastAPI(title="Training-job queue")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
