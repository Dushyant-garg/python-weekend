from sqlalchemy import Column, Integer, String, Date, ForeignKey, Text
from .database import Base
from sqlalchemy.orm import relationship

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String, default="member")  
    points = Column(Integer, default=100) 
    feedbacks = relationship("Feedback", back_populates="user")

class Paper(Base):
    __tablename__ = "papers"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    author = Column(String, nullable=False)
    publication_date = Column(Date, nullable=False)
    filename = Column(String, nullable=False)
    uploader_id = Column(Integer, ForeignKey("users.id"))

    uploader = relationship("User", back_populates="papers")
    feedbacks = relationship("Feedback", back_populates="paper")

User.papers = relationship("Paper", back_populates="uploader", cascade="all, delete-orphan")

class Feedback(Base):
    __tablename__ = "feedbacks"

    id = Column(Integer, primary_key=True, index=True)
    comment = Column(Text, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"))
    paper_id = Column(Integer, ForeignKey("papers.id"))

    user = relationship("User", back_populates="feedbacks")
    paper = relationship("Paper", back_populates="feedbacks")