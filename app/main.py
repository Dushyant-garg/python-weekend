from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from . import models, schemas, utils, database
from datetime import timedelta
from app.auth.auth_routes import router as auth_router
from app.paper.paper_routes import router as paper_router

models.Base.metadata.create_all(bind=database.engine)

app = FastAPI()

app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(paper_router, prefix="/papers", tags=["Papers"])
