from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime
from ..models.file import FileStatus

class FileBase(BaseModel):
    original_filename: str
    content_type: Optional[str] = None
    file_size: int


class FileCreate(FileBase):
    stored_filename: str
    file_path: str


class FileUpdate(BaseModel):
    status: Optional[FileStatus] = None


class FileInDBBase(FileBase):
    id: int
    uuid: UUID
    status: FileStatus
    created_at: datetime

    class Config:
        from_attributes = True

# The final response model sent back to the client
class FileResponse(FileInDBBase):
    pass