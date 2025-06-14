from fastapi import FastAPI
from router.PromisedDate_SerialStatus_router import router as PromisedDate_SerialStatus_router



app = FastAPI()

# Incluir el router de PromisedDate_SerialStatus_router
app.include_router(PromisedDate_SerialStatus_router)