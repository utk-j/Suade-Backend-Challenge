from fastapi import FastAPI
from app.routes.upload import router as upload_router
from app.routes.summary import router as summary_router

app = FastAPI(title="Suade Backend Challenge")

@app.get("/")
def root():
    return {"message": "API running"}

app.include_router(upload_router)
app.include_router(summary_router)
