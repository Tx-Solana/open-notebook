import asyncio
import subprocess
import time
from typing import Optional
from loguru import logger
from surreal_commands import submit_command


class WorkerManager:
    """Manages worker lifecycle for cost optimization using supervisor"""
    
    def __init__(self, idle_timeout: int = 300):  # 5 minutes default
        self.idle_timeout = idle_timeout
        self.last_job_time = time.time()
        self._monitor_process: Optional[subprocess.Popen] = None
    
    async def _wait_for_database(self, max_retries: int = 10, initial_delay: float = 1.0) -> bool:
        """Wait for database to be ready with exponential backoff"""
        for attempt in range(max_retries):
            try:
                # Simple test to see if we can connect to SurrealDB
                from surreal_commands import registry
                
                # Try to get registry info (this requires DB connection)
                commands = registry.list_commands()
                logger.info(f"Database ready, found {len(commands)} registered commands")
                return True
                
            except Exception as e:
                delay = initial_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Database not ready (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.error("Database failed to become ready after maximum retries")
                    return False
        
        return False
    
    async def ensure_worker_running(self, max_retries: int = 3) -> bool:
        """Start worker via supervisor if not running, with retry logic for robustness"""
        for attempt in range(max_retries):
            try:
                # Check if worker is already running
                result = subprocess.run(
                    ["supervisorctl", "status", "worker"],
                    capture_output=True, text=True, timeout=5
                )
                
                if "RUNNING" in result.stdout:
                    logger.debug("Worker already running via supervisor")
                    return True
                
                # Try to start worker
                logger.info(f"Starting worker via supervisor (attempt {attempt + 1}/{max_retries})...")
                start_result = subprocess.run(
                    ["supervisorctl", "start", "worker"],
                    capture_output=True, text=True, timeout=10
                )
                
                if start_result.returncode != 0:
                    logger.warning(f"Supervisor failed to start worker: {start_result.stderr}")
                    if attempt < max_retries - 1:
                        delay = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.info(f"Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Failed to start worker after all attempts")
                        return False
                
                # Wait a moment for worker to initialize, then verify it's actually running
                await asyncio.sleep(3)
                status_result = subprocess.run(
                    ["supervisorctl", "status", "worker"],
                    capture_output=True, text=True, timeout=5
                )
                
                if "RUNNING" in status_result.stdout:
                    logger.info(f"Worker started successfully on attempt {attempt + 1}")
                    self.last_job_time = time.time()
                    await self._ensure_monitor_running()
                    return True
                else:
                    # Worker started but crashed/exited
                    logger.warning(f"Worker started but crashed (attempt {attempt + 1}): {status_result.stdout}")
                    if attempt < max_retries - 1:
                        delay = 2 ** attempt
                        logger.info(f"Worker crashed, retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Worker failed to stay running after all attempts")
                        return False
                        
            except Exception as e:
                logger.warning(f"Error during worker start attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.info(f"Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error("Failed to start worker due to exceptions")
                    return False
        
        return False
    
    async def submit_job_with_worker(self, app_name: str, command_name: str, args: dict) -> str:
        """Submit job and ensure worker is running with monitoring"""
        # Start worker if needed
        await self.ensure_worker_running()
        
        # Submit the job
        job_id = submit_command(app_name, command_name, args)
        # Convert RecordID to string if needed
        job_id_str = str(job_id) if job_id else None
        self.last_job_time = time.time()
        
        logger.info(f"Job submitted: {job_id_str}, worker monitoring active")
        return job_id_str
    
    async def _ensure_monitor_running(self):
        """Start monitor process via supervisor if not already running"""
        try:
            # Check if monitor is already running via supervisor
            result = subprocess.run(
                ["supervisorctl", "status", "worker-monitor"],
                capture_output=True, text=True, timeout=5
            )
            
            if "RUNNING" in result.stdout:
                logger.debug("Worker monitor already running via supervisor")
                return
            
            logger.info(f"Starting worker monitor via supervisor with {self.idle_timeout}s timeout...")
            
            # Update the supervisor program with current timeout (if needed)
            # For now, just start with default timeout configured in supervisor
            start_result = subprocess.run(
                ["supervisorctl", "start", "worker-monitor"],
                capture_output=True, text=True, timeout=10
            )
            
            if start_result.returncode == 0:
                logger.info("Worker monitor started successfully via supervisor")
            else:
                logger.warning(f"Could not start monitor via supervisor: {start_result.stderr}")
                # Fallback to direct process start
                await self._start_monitor_directly()
            
        except Exception as e:
            logger.warning(f"Failed to start monitor via supervisor: {e}")
            # Fallback to direct process start
            await self._start_monitor_directly()
    
    async def _start_monitor_directly(self):
        """Fallback: Start monitor as direct subprocess"""
        try:
            logger.info(f"Starting worker monitor directly with {self.idle_timeout}s timeout...")
            self._monitor_process = subprocess.Popen([
                "/app/scripts/smart-worker-monitor.sh", 
                str(self.idle_timeout)
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"Worker monitor started directly with PID: {self._monitor_process.pid}")
            
        except Exception as e:
            logger.error(f"Failed to start worker monitor directly: {e}")
    
    async def stop_worker_and_monitor(self):
        """Stop both worker and monitor"""
        try:
            # Stop worker via supervisor
            subprocess.run(
                ["supervisorctl", "stop", "worker"],
                capture_output=True, timeout=10
            )
            logger.info("Worker stopped via supervisor")
            
            # Try to stop monitor via supervisor first
            try:
                subprocess.run(
                    ["supervisorctl", "stop", "worker-monitor"],
                    capture_output=True, timeout=5
                )
                logger.info("Monitor stopped via supervisor")
            except Exception:
                # Fallback to direct process termination
                if self._monitor_process and self._monitor_process.poll() is None:
                    self._monitor_process.terminate()
                    logger.info("Monitor process terminated directly")
                
        except Exception as e:
            logger.error(f"Error stopping worker/monitor: {e}")


# Global worker manager instance
worker_manager = WorkerManager(idle_timeout=300)  # 5 minutes


async def submit_background_job(app_name: str, command_name: str, args: dict) -> str:
    """Public interface for submitting background jobs with auto-worker management"""
    return await worker_manager.submit_job_with_worker(app_name, command_name, args)
