from enum import Enum as PyEnum
from sqlalchemy import Column, Integer, String, Enum, ForeignKey, DateTime, Boolean,Text,JSON,UniqueConstraint,BigInteger
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class JobStatus(str, PyEnum):
    queued = "queued"
    parsing = "parsing"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"
    
class BatchStatus(str, PyEnum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    
class ItemStatus(str, PyEnum):
    pending = "pending"
    processed = "processed"
    invalid = "invalid"
    failed = "failed"
    
    
class Job(Base):
    __tablename__ = "jobs"

    id = Column(BigInteger, primary_key=True, index=True)
    filename = Column(String(255), nullable = False)
    file_type = Column(String(32), nullable = False)
    file_uri = Column(String(1024), nullable = False)
    batch_size = Column(Integer, nullable = False, default=20)
    total_items = Column(Integer, nullable=False, default=0)
    processed_items = Column(Integer, nullable=False, default=0)
    failed_items = Column(Integer, nullable=False, default=0)
    invalid_items = Column(Integer, nullable=False, default=0)
    status = Column(Enum(JobStatus), default=JobStatus.queued, nullable=False)
    eta_seconds = Column(Integer, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default="now()")
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    
    batches = relationship("Batch", back_populates="job", cascade="all, delete-orphan") 
    
    
class Batch(Base):
    __tablename__ = "batches"

    id = Column(BigInteger, primary_key=True, index=True)
    job_id = Column(BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    index = Column(Integer, nullable=False)
    status = Column(Enum(BatchStatus), default=BatchStatus.queued, nullable=False)
    total_items = Column(Integer, nullable=False, default=0)
    processed_items = Column(Integer, nullable=False, default=0)
    failed_items = Column(Integer, nullable=False, default=0)
    invalid_items = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, server_default="now()")
    error = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    
    job = relationship("Job", back_populates="batches")
    items = relationship("Item", back_populates="batch", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint('job_id', 'index', name='_job_index_uc'),)
    
    
class Item(Base):
    __tablename__ = "items"
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_id = Column(BigInteger, ForeignKey("batches.id", ondelete="CASCADE"), nullable=False, index=True)
    job_id = Column(BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String(320), nullable=False)
    payload = Column(JSONB, nullable=True)
    status = Column(Enum(ItemStatus), default=ItemStatus.pending, nullable=False)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    
    batch = relationship("Batch", back_populates="items")
    
    
class ProcessingEvents(Base):
    __tablename__ = "processing_events"
    
    id = Column(BigInteger, primary_key=True, index=True)
    job_id = Column(BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False,index=True)
    batch_id = Column(BigInteger, ForeignKey("batches.id",ondelete="CASCADE"), nullable=True,index=True)
    level = Column(String(16), nullable=False)
    code = Column(String(64), nullable=False)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)