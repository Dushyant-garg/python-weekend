from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app import schemas, models, database
from app.database import get_db
from app.models import User
from app.schemas import UserResponse, UserCreate, UpdatePointsRequest, UpdateRoleRequest
from .auth_service import authenticate_user, hash_password, generate_jwt, admin_required, get_current_user

router = APIRouter()

@router.post("/register", response_model=schemas.UserResponse)
def register(user: schemas.UserCreate, db: Session = Depends(database.get_db)):
    existing = db.query(models.User).filter(models.User.email == user.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    new_user = models.User(
        username=user.username,
        email=user.email,
        hashed_password=hash_password(user.password)
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

@router.post("/login", response_model=schemas.TokenResponse)
def login(user: schemas.LoginRequest, db: Session = Depends(database.get_db)):
    auth_user = authenticate_user(user.username, user.password, db)
    if not auth_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = generate_jwt({"sub": auth_user.username})
    return {"access_token": token, "token_type": "bearer"}

@router.get("/users", response_model=list[schemas.UserResponse])
def get_all_users(db: Session = Depends(database.get_db), _: models.User = Depends(admin_required)):
    return db.query(models.User).all()

@router.get("/users/{user_id}", response_model=schemas.UserResponse)
def get_user(user_id: int, db: Session = Depends(database.get_db), _: models.User = Depends(admin_required)):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.patch("/admin/users/{user_id}", response_model=UserResponse)
def update_user_role(
    user_id: int,
    data: UpdateRoleRequest,
    db: Session = Depends(get_db),
    _: User = Depends(admin_required)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = data.role
    db.commit()
    db.refresh(user)
    return user

# Update Points
@router.patch("/admin/add-points-to-user/{user_id}", response_model=UserResponse)
def update_user_points(
    user_id: int,
    data: UpdatePointsRequest,
    db: Session = Depends(get_db),
    _: User = Depends(admin_required)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.points = user.points + data.points
    db.commit()
    db.refresh(user)
    return user