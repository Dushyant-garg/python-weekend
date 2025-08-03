import os
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date, datetime
from app.models import Paper, User, Feedback
from app.database import get_db
from app.auth.auth_service import researcher_or_admin_required, get_current_user
from app.schemas import PaperUploadRequest, FeedbackResponse, FeedbackCreate
from fastapi.responses import FileResponse

router = APIRouter()

UPLOAD_DIR = "uploads"

@router.post("/upload")
async def upload_paper(
    title: str = Form(...),
    author: str = Form(...),
    publicationDate: date = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(researcher_or_admin_required),
):
    # Validate date
    if publicationDate > date.today():
        raise HTTPException(status_code=400, detail="Publication date cannot be in the future.")

    # Validate file type
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")
    
    author_user = db.query(User).filter(User.username == author).first()
    if not author_user:
        raise HTTPException(status_code=400, detail="Author does not exist in the system.")

    # Determine path
    if current_user.role == "admin":
        dest_dir = os.path.join(UPLOAD_DIR, "official")
    else:
        dest_dir = os.path.join(UPLOAD_DIR, author)

    os.makedirs(dest_dir, exist_ok=True)

    filename = f"{datetime.utcnow().timestamp()}_{file.filename}"
    file_path = os.path.join(dest_dir, filename)

    # Save file
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)

    # Create DB entry
    paper = Paper(
        title=title,
        author=author,
        publication_date=publicationDate,
        filename=file_path,
        uploader_id=current_user.id,
    )
    db.add(paper)

    # Award points if researcher
    if current_user.role == "researcher":
        current_user.points += 100

    db.commit()
    db.refresh(paper)

    return {
        "message": "Paper uploaded successfully",
        "paper_id": paper.id,
        "stored_at": file_path
    }

@router.get("/download/{paper_id}")
def download_paper(
    paper_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Fetch the paper
    paper = db.query(Paper).filter(Paper.id == paper_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found.")

    # Check user's points
    if current_user.points < 10:
        raise HTTPException(status_code=402, detail="Payment required.")

    # Check if file exists
    if not os.path.exists(paper.filename):
        raise HTTPException(status_code=404, detail="PDF file not found on server.")

    # Deduct 10 points
    current_user.points -= 10
    db.commit()

    # Serve the file
    return FileResponse(
        paper.filename,
        media_type="application/pdf",
        filename=os.path.basename(paper.filename)
    )

@router.post("/{paper_id}/feedback", response_model=FeedbackResponse)
def add_feedback(
    paper_id: int,
    feedback_data: FeedbackCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    # Ensure paper exists
    paper = db.query(Paper).filter(Paper.id == paper_id).first()
    if not paper:
        raise HTTPException(status_code=404, detail="Paper not found.")

    # Create feedback
    feedback = Feedback(
        comment=feedback_data.comment,
        user_id=current_user.id,
        paper_id=paper.id
    )
    db.add(feedback)

    # Reward user with 5 points
    current_user.points += 5

    db.commit()
    db.refresh(feedback)

    return feedback
