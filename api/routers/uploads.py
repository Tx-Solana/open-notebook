import os
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from loguru import logger

from api.models import FileUploadResponse
from open_notebook.config import UPLOADS_FOLDER

router = APIRouter()


@router.post("/uploads/file", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    custom_filename: Optional[str] = Form(None),
    preserve_original_name: bool = Form(False)
):
    """
    Upload a file and save it to the uploads folder.
    
    Args:
        file: The uploaded file
        custom_filename: Optional custom filename (without extension)
        preserve_original_name: If True, keeps original filename (may cause conflicts)
    
    Returns:
        FileUploadResponse with file path and metadata
    """
    try:
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")
        
        # Get file info
        original_filename = file.filename
        file_extension = Path(original_filename).suffix
        base_name = Path(original_filename).stem
        
        # Determine final filename
        if custom_filename:
            # Use custom filename with original extension
            final_filename = f"{custom_filename}{file_extension}"
        elif preserve_original_name:
            # Use original filename (may cause conflicts)
            final_filename = original_filename
        else:
            # Generate unique filename with UUID prefix
            unique_id = str(uuid.uuid4())[:8]
            final_filename = f"{unique_id}_{base_name}{file_extension}"
        
        # Generate unique file path if file already exists
        file_path = os.path.join(UPLOADS_FOLDER, final_filename)
        counter = 0
        while os.path.exists(file_path):
            counter += 1
            name_without_ext = Path(final_filename).stem
            final_filename_with_counter = f"{name_without_ext}_{counter}{file_extension}"
            file_path = os.path.join(UPLOADS_FOLDER, final_filename_with_counter)
            final_filename = final_filename_with_counter
        
        # Save file
        file_content = await file.read()
        with open(file_path, "wb") as f:
            f.write(file_content)
        
        file_size = len(file_content)
        
        logger.info(f"File uploaded successfully: {original_filename} -> {file_path}")
        
        return FileUploadResponse(
            file_path=str(file_path),
            original_filename=original_filename,
            saved_filename=final_filename,
            file_size=file_size,
            message=f"File '{original_filename}' uploaded successfully as '{final_filename}'"
        )
        
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.post("/uploads/file-from-binary", response_model=FileUploadResponse)
async def upload_file_from_binary(
    filename: str = Form(...),
    file_data: bytes = Form(...),
    custom_filename: Optional[str] = Form(None)
):
    """
    Upload a file from raw binary data (for n8n workflows).
    
    Args:
        filename: Original filename
        file_data: Raw binary file data
        custom_filename: Optional custom filename (without extension)
    
    Returns:
        FileUploadResponse with file path and metadata
    """
    try:
        # Get file info
        file_extension = Path(filename).suffix
        base_name = Path(filename).stem
        
        # Determine final filename
        if custom_filename:
            final_filename = f"{custom_filename}{file_extension}"
        else:
            # Generate unique filename with UUID prefix
            unique_id = str(uuid.uuid4())[:8]
            final_filename = f"{unique_id}_{base_name}{file_extension}"
        
        # Generate unique file path if file already exists
        file_path = os.path.join(UPLOADS_FOLDER, final_filename)
        counter = 0
        while os.path.exists(file_path):
            counter += 1
            name_without_ext = Path(final_filename).stem
            final_filename_with_counter = f"{name_without_ext}_{counter}{file_extension}"
            file_path = os.path.join(UPLOADS_FOLDER, final_filename_with_counter)
            final_filename = final_filename_with_counter
        
        # Save file
        with open(file_path, "wb") as f:
            f.write(file_data)
        
        file_size = len(file_data)
        
        logger.info(f"File uploaded from binary data: {filename} -> {file_path}")
        
        return FileUploadResponse(
            file_path=str(file_path),
            original_filename=filename,
            saved_filename=final_filename,
            file_size=file_size,
            message=f"File '{filename}' uploaded successfully as '{final_filename}'"
        )
        
    except Exception as e:
        logger.error(f"Error uploading file from binary data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading file from binary data: {str(e)}")
