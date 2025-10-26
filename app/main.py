from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.routes.upload import router as upload_router
from app.routes.summary import router as summary_router
from app.utils import state


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.ensure_data_layout()
    state.try_restore_dataset_path()
    yield

app = FastAPI(title="Suade Backend Challenge", lifespan=lifespan)

@app.get("/")
def root():
    return {"message": "API running"}


app.include_router(upload_router)
app.include_router(summary_router)
