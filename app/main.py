from fastapi import FastAPI
from app.routes.upload import router as upload_router
from app.routes.summary import router as summary_router
from app.utils import state

app = FastAPI(title="Suade Backend Challenge")

@app.on_event("startup")
def restore_dataset():
    state.ensure_data_layout()           
    state.try_restore_dataset_path()     

@app.get("/")
def root():
    return {"message": "API running"}

app.include_router(upload_router)
app.include_router(summary_router)
