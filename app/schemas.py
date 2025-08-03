from pydantic import BaseModel, EmailStr, Field
from datetime import date

class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    
class UserResponse(BaseModel):
    id: int
    username: str
    email: EmailStr
    role: str
    points: int

    class Config:
        orm_mode = True

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UpdateRoleRequest(BaseModel):
    role: str = Field(..., pattern="^(member|researcher|admin)$")

class UpdatePointsRequest(BaseModel):
    points: int = Field(..., ge=0) 

class PaperUploadRequest(BaseModel):
    title: str
    author: str
    publicationDate: date

class FeedbackCreate(BaseModel):
    comment: str

class FeedbackResponse(BaseModel):
    id: int
    comment: str
    user_id: int
    paper_id: int

    class Config:
        orm_mode = True