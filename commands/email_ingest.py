from surreal_commands import command
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from loguru import logger
import asyncio
import time
import imaplib
import email
from email.header import decode_header
import aiohttp
import os

logger.info("=== IMPORTING email_commands.py ===")
logger.info("Registering email commands...")

class EmailConfig(BaseModel):
    server: str
    username: str
    password: str
    folder: str = "INBOX"
    search_criteria: str = "UNSEEN"
    mark_as_read: bool = True
    port: int = 993
    use_ssl: bool = True

class EmailIngestionInput(BaseModel):
    notebook_id: str
    email_config: EmailConfig
    max_emails: Optional[int] = 50  # Limit number of emails to process
    delay_seconds: Optional[int] = None  # For testing

class EmailData(BaseModel):
    subject: str
    sender: str
    date: str
    body: str
    message_id: str

class EmailIngestionOutput(BaseModel):
    success: bool
    emails_processed: int
    sources_created: List[str] = []
    processing_time: float
    error_message: Optional[str] = None
    emails_data: List[EmailData] = []

@command("email_ingest", app="open_notebook")
async def email_ingest_command(input_data: EmailIngestionInput) -> EmailIngestionOutput:
    """
    Ingest emails from IMAP server into a notebook as sources.
    Fetches unread emails and creates sources for each one.
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting email ingestion for notebook: {input_data.notebook_id}")
        logger.info(f"Email server: {input_data.email_config.server}")
        
        # Simulate processing delay if specified (for testing)
        if input_data.delay_seconds:
            await asyncio.sleep(input_data.delay_seconds)
        
        # Fetch emails from IMAP server
        emails_data = await _fetch_emails_from_imap(input_data.email_config, input_data.max_emails)
        logger.info(f"Fetched {len(emails_data)} emails")
        
        # Create sources for each email
        sources_created = []
        for email_data in emails_data:
            try:
                source_id = await _create_email_source(input_data.notebook_id, email_data)
                if source_id:
                    sources_created.append(source_id)
                    logger.info(f"Created source {source_id} for email: {email_data.subject[:50]}...")
            except Exception as e:
                logger.error(f"Failed to create source for email {email_data.subject}: {e}")
                # Continue processing other emails
        
        processing_time = time.time() - start_time
        
        return EmailIngestionOutput(
            success=True,
            emails_processed=len(emails_data),
            sources_created=sources_created,
            processing_time=processing_time,
            emails_data=emails_data
        )
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Email ingestion failed: {e}")
        return EmailIngestionOutput(
            success=False,
            emails_processed=0,
            processing_time=processing_time,
            error_message=str(e)
        )

async def _fetch_emails_from_imap(email_config: EmailConfig, max_emails: int) -> List[EmailData]:
    """Fetch emails from IMAP server"""
    emails = []
    
    try:
        # Connect to IMAP server
        if email_config.use_ssl:
            mail = imaplib.IMAP4_SSL(email_config.server, email_config.port)
        else:
            mail = imaplib.IMAP4(email_config.server, email_config.port)
        
        mail.login(email_config.username, email_config.password)
        mail.select(email_config.folder)
        
        # Search for emails
        status, messages = mail.search(None, email_config.search_criteria)
        
        if status != 'OK':
            raise Exception(f"Failed to search emails: {status}")
        
        message_ids = messages[0].split()
        
        # Limit number of emails to process
        if max_emails and len(message_ids) > max_emails:
            message_ids = message_ids[-max_emails:]  # Get most recent emails
        
        for msg_id in message_ids:
            try:
                status, msg_data = mail.fetch(msg_id, '(RFC822)')
                if status != 'OK':
                    continue
                    
                email_body = msg_data[0][1]
                email_message = email.message_from_bytes(email_body)
                
                # Extract email data
                subject = _decode_header(email_message["Subject"]) or "No Subject"
                sender = email_message.get("From", "Unknown Sender")
                date = email_message.get("Date", "Unknown Date")
                body = _extract_email_body(email_message)
                message_id = email_message.get("Message-ID", f"no-id-{msg_id}")
                
                emails.append(EmailData(
                    subject=subject,
                    sender=sender,
                    date=date,
                    body=body,
                    message_id=message_id
                ))
                
                # Mark as seen if configured
                if email_config.mark_as_read:
                    mail.store(msg_id, '+FLAGS', '\\Seen')
                    
            except Exception as e:
                logger.error(f"Failed to process email {msg_id}: {e}")
                continue
        
        mail.close()
        mail.logout()
        
    except Exception as e:
        logger.error(f"IMAP connection failed: {e}")
        raise
    
    return emails

def _decode_header(header):
    """Decode email header"""
    if not header:
        return ""
    
    try:
        decoded = decode_header(header)[0]
        if isinstance(decoded[0], bytes):
            return decoded[0].decode(decoded[1] or 'utf-8')
        return str(decoded[0])
    except Exception as e:
        logger.error(f"Failed to decode header: {e}")
        return str(header)

def _extract_email_body(email_message):
    """Extract email body text"""
    body = ""
    try:
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode('utf-8', errors='ignore')
                        break
                elif part.get_content_type() == "text/html" and not body:
                    # Fallback to HTML if no plain text
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode('utf-8', errors='ignore')
        else:
            payload = email_message.get_payload(decode=True)
            if payload:
                body = payload.decode('utf-8', errors='ignore')
    except Exception as e:
        logger.error(f"Failed to extract email body: {e}")
        body = "Failed to extract email content"
    
    return body

async def _create_email_source(notebook_id: str, email_data: EmailData) -> Optional[str]:
    """Create a source from email data via the Sources API and apply transformations"""
    try:
        # Format email content
        content = f"""Subject: {email_data.subject}
From: {email_data.sender}
Date: {email_data.date}
Message-ID: {email_data.message_id}

{email_data.body}
"""
        
        # Dense Summary transformation ID
        dense_summary_id = "transformation:ciqyp8834jwqo81p7901"
        
        source_data = {
            "notebook_id": notebook_id,
            "title": f"Email: {email_data.subject}",
            "content": content,
            "type": "text",
            "transformations": [dense_summary_id],  # Apply dense summary
            "embed": True  # Enable embedding for search
        }
        
        # Get API base URL and password from environment
        api_base = os.getenv("API_BASE_URL", "http://localhost:5055")
        password = os.getenv("API_PASSWORD", "")
        
        headers = {}
        if password:
            headers["Authorization"] = f"Bearer {password}"
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{api_base}/api/sources",
                headers=headers,
                json=source_data
            ) as response:
                if response.status == 200 or response.status == 201:
                    result = await response.json()
                    source_id = result.get("id")
                    
                    # Update the source title since text sources don't get titles automatically
                    email_title = f"Email: {email_data.subject}"
                    update_data = {"title": email_title}
                    
                    async with session.put(
                        f"{api_base}/api/sources/{source_id}",
                        headers=headers,
                        json=update_data
                    ) as update_response:
                        if update_response.status == 200:
                            logger.info(f"Created and titled email source {source_id}: {email_title}")
                        else:
                            logger.warning(f"Created source {source_id} but failed to set title")
                    
                    return source_id
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create source: {response.status} - {error_text}")
                    return None
                    
    except Exception as e:
        logger.error(f"Failed to create email source: {e}")
        return None

# Enhanced command for testing the complete email ingestion process
class EmailDryRunInput(BaseModel):
    email_config: EmailConfig
    max_emails: Optional[int] = 5  # Limit for dry run
    notebook_id: Optional[str] = "dry-run-test"  # Optional test notebook

class EmailPreview(BaseModel):
    subject: str
    sender: str
    date: str
    body_preview: str  # First 200 chars
    message_id: str
    body_length: int

class EmailDryRunOutput(BaseModel):
    success: bool
    message: str
    connection_test: bool
    emails_found: int
    emails_processed: int
    email_previews: List[EmailPreview] = []
    api_test: bool
    source_data_examples: List[Dict[str, Any]] = []
    processing_time: float
    error_message: Optional[str] = None
    warnings: List[str] = []

@command("email_test", app="open_notebook")
async def email_dry_run_command(input_data: EmailDryRunInput) -> EmailDryRunOutput:
    """
    Complete dry run of email ingestion process. Tests connection, fetches emails,
    processes them, and simulates source creation without actually creating sources.
    Provides detailed preview of what would be ingested.
    """
    start_time = time.time()
    warnings = []
    
    try:
        logger.info(f"Starting email dry run for: {input_data.email_config.server}")
        
        # Step 1: Test connection
        logger.info("Step 1: Testing IMAP connection...")
        connection_test = True
        try:
            if input_data.email_config.use_ssl:
                mail = imaplib.IMAP4_SSL(input_data.email_config.server, input_data.email_config.port)
            else:
                mail = imaplib.IMAP4(input_data.email_config.server, input_data.email_config.port)
            
            mail.login(input_data.email_config.username, input_data.email_config.password)
            mail.select(input_data.email_config.folder)
            logger.info("✅ IMAP connection successful")
        except Exception as e:
            logger.error(f"❌ IMAP connection failed: {e}")
            return EmailDryRunOutput(
                success=False,
                message="IMAP connection failed",
                connection_test=False,
                emails_found=0,
                emails_processed=0,
                api_test=False,
                processing_time=time.time() - start_time,
                error_message=str(e)
            )
        
        # Step 2: Search and count emails
        logger.info(f"Step 2: Searching for emails with criteria: {input_data.email_config.search_criteria}")
        status, messages = mail.search(None, input_data.email_config.search_criteria)
        
        if status != 'OK':
            warnings.append(f"Search returned status: {status}")
        
        message_ids = messages[0].split() if messages[0] else []
        emails_found = len(message_ids)
        logger.info(f"📧 Found {emails_found} emails matching criteria")
        
        if emails_found == 0:
            # Try with "ALL" to see if there are any emails at all
            status, all_messages = mail.search(None, "ALL")
            all_count = len(all_messages[0].split()) if all_messages[0] else 0
            if all_count > 0:
                warnings.append(f"No emails found with criteria '{input_data.email_config.search_criteria}', but {all_count} total emails exist")
            else:
                warnings.append("No emails found in mailbox at all")
        
        # Step 3: Process a limited number of emails
        emails_to_process = min(input_data.max_emails or 5, emails_found)
        logger.info(f"Step 3: Processing {emails_to_process} emails for preview...")
        
        email_previews = []
        source_data_examples = []
        
        if emails_to_process > 0:
            # Get most recent emails
            recent_ids = message_ids[-emails_to_process:] if len(message_ids) > emails_to_process else message_ids
            
            for i, msg_id in enumerate(recent_ids):
                try:
                    logger.info(f"Processing email {i+1}/{emails_to_process}")
                    status, msg_data = mail.fetch(msg_id, '(RFC822)')
                    
                    if status != 'OK':
                        warnings.append(f"Failed to fetch email {msg_id}: {status}")
                        continue
                    
                    email_body = msg_data[0][1]
                    email_message = email.message_from_bytes(email_body)
                    
                    # Extract email data
                    subject = _decode_header(email_message["Subject"]) or "No Subject"
                    sender = email_message.get("From", "Unknown Sender")
                    date = email_message.get("Date", "Unknown Date")
                    body = _extract_email_body(email_message)
                    message_id = email_message.get("Message-ID", f"no-id-{msg_id}")
                    
                    # Create preview
                    email_preview = EmailPreview(
                        subject=subject,
                        sender=sender,
                        date=date,
                        body_preview=body[:200] + "..." if len(body) > 200 else body,
                        message_id=message_id,
                        body_length=len(body)
                    )
                    email_previews.append(email_preview)
                    
                    # Create example source data
                    content = f"""Subject: {subject}
                        From: {sender}
                        Date: {date}
                        Message-ID: {message_id}

                        {body}
                        """
                    
                    source_data = {
                        "notebook_id": input_data.notebook_id,
                        "title": f"Email: {subject}",
                        "content": content,
                        "source_type": "email",
                        "metadata": {
                            "sender": sender,
                            "date": date,
                            "message_id": message_id,
                            "subject": subject,
                            "body_length": len(body)
                        }
                    }
                    source_data_examples.append(source_data)
                    
                except Exception as e:
                    logger.error(f"Failed to process email {msg_id}: {e}")
                    warnings.append(f"Failed to process email {msg_id}: {str(e)}")
                    continue
        
        mail.close()
        mail.logout()
        
        # Step 4: Test API connectivity (without creating sources)
        logger.info("Step 4: Testing API connectivity...")
        api_test = True
        try:
            import os
            import aiohttp
            
            api_base = os.getenv("API_BASE_URL", "http://localhost:5055")
            password = os.getenv("API_PASSWORD", "")
            
            headers = {}
            if password:
                headers["Authorization"] = f"Bearer {password}"
            
            async with aiohttp.ClientSession() as session:
                # Test with a simple health check or notebooks endpoint
                async with session.get(f"{api_base}/health", headers=headers) as response:
                    if response.status == 200:
                        logger.info("✅ API connectivity test successful")
                    else:
                        logger.warning(f"API responded with status: {response.status}")
                        warnings.append(f"API health check returned status: {response.status}")
        except Exception as e:
            logger.error(f"❌ API connectivity test failed: {e}")
            api_test = False
            warnings.append(f"API connectivity test failed: {str(e)}")
        
        processing_time = time.time() - start_time
        
        # Create summary message
        message_parts = [
            f"✅ Connection successful to {input_data.email_config.server}",
            f"📧 Found {emails_found} emails with criteria '{input_data.email_config.search_criteria}'",
            f"🔍 Processed {len(email_previews)} emails for preview",
        ]
        
        if api_test:
            message_parts.append("🌐 API connectivity confirmed")
        
        if warnings:
            message_parts.append(f"⚠️  {len(warnings)} warnings (see warnings field)")
        
        return EmailDryRunOutput(
            success=True,
            message=" | ".join(message_parts),
            connection_test=connection_test,
            emails_found=emails_found,
            emails_processed=len(email_previews),
            email_previews=email_previews,
            api_test=api_test,
            source_data_examples=source_data_examples,
            processing_time=processing_time,
            warnings=warnings
        )
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Email dry run failed: {e}")
        return EmailDryRunOutput(
            success=False,
            message="Email dry run failed",
            connection_test=False,
            emails_found=0,
            emails_processed=0,
            api_test=False,
            processing_time=processing_time,
            error_message=str(e),
            warnings=warnings
        )

logger.info("✅ Email commands registered: email_ingest and email_test")
logger.info("=== FINISHED IMPORTING email_commands.py ===")