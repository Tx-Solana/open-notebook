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
    
    async def ensure_worker_running(self) -> bool:
        """Start worker via supervisor if not running"""
        try:
            # Check if worker is already running
            result = subprocess.run(
                ["supervisorctl", "status", "worker"],
                capture_output=True, text=True, timeout=5
            )
            
            if "RUNNING" in result.stdout:
                logger.debug("Worker already running via supervisor")
                return True
            
            logger.info("Starting worker via supervisor...")
            start_result = subprocess.run(
                ["supervisorctl", "start", "worker"],
                capture_output=True, text=True, timeout=10
            )
            
            if start_result.returncode == 0:
                logger.info("Worker started successfully via supervisor")
                self.last_job_time = time.time()
                
                # Start monitor if not already running
                await self._ensure_monitor_running()
                return True
            else:
                logger.error(f"Failed to start worker: {start_result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to start worker via supervisor: {e}")
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
