from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Agent2 API")

@app.get("/health")
def health():
    return {"status": "ok"}
@app.get("/")
def root():
    return JSONResponse(content={"message": "Agent2 backend running successfully"})


