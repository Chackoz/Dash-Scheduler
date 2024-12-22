import firebase_admin
from firebase_admin import credentials, db
import tempfile
import subprocess
import os
import logging
from datetime import datetime
import asyncio
from typing import Dict, Any
import queue
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Firebase configuration
FIREBASE_CONFIG = {
    'apiKey': "AIzaSyAkmPcECEHT4TN06hCxJMc51G4TYn6Hsbs",
    'authDomain': "dash-b26cb.firebaseapp.com",
    'projectId': "dash-b26cb",
    'storageBucket': "dash-b26cb.appspot.com",
    'messagingSenderId': "684408724522",
    'appId': "1:684408724522:web:f137155fef779d0bb47753",
    'measurementId': "G-3XMMJBN996",
    'databaseURL': "https://dash-b26cb-default-rtdb.firebaseio.com"
}

class DashScheduler:
    def __init__(self, cred_path: str, worker_id: str):
        """Initialize the DASH scheduler"""
        # Initialize Firebase
        try:
            firebase_admin.get_app()
        except ValueError:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {
                'databaseURL': FIREBASE_CONFIG['databaseURL']
            })
        
        self.worker_id = worker_id
        self.processing = False
        self.task_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()

    async def process_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Process a single task"""
        if task_data.get('status') != 'pending':
            return

        logger.info(f"Processing task {task_id}")
        task_ref = db.reference(f'tasks/{task_id}')
        
        self.processing = True
        try:
            # Update task status to running
            task_ref.update({
                'status': 'running',
                'workerId': self.worker_id,
                'updatedAt': datetime.now().isoformat()
            })

            # Create temporary file for code execution
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
                temp_file.write(task_data['code'])
                temp_path = temp_file.name

            try:
                # Execute the code
                proc = await asyncio.create_subprocess_exec(
                    'python', temp_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
                    
                    if proc.returncode == 0:
                        # Success
                        task_ref.update({
                            'status': 'completed',
                            'output': stdout.decode(),
                            'updatedAt': datetime.now().isoformat()
                        })
                        logger.info(f"Task {task_id} completed successfully")
                    else:
                        # Failure
                        task_ref.update({
                            'status': 'failed',
                            'output': f"Error:\n{stderr.decode()}",
                            'updatedAt': datetime.now().isoformat()
                        })
                        logger.error(f"Task {task_id} failed with error: {stderr.decode()}")

                except asyncio.TimeoutError:
                    proc.kill()
                    task_ref.update({
                        'status': 'failed',
                        'output': 'Task execution timed out after 30 seconds',
                        'updatedAt': datetime.now().isoformat()
                    })
                    logger.error(f"Task {task_id} timed out")

            finally:
                # Clean up temporary file
                os.unlink(temp_path)

        except Exception as e:
            task_ref.update({
                'status': 'failed',
                'output': f"Error: {str(e)}",
                'updatedAt': datetime.now().isoformat()
            })
            logger.error(f"Error processing task {task_id}: {str(e)}")
        
        finally:
            self.processing = False

    def _task_callback(self, event):
        """Callback for Firebase task events"""
        if event.data and event.path:
            task_id = event.path[1:]  # Remove leading slash
            asyncio.run_coroutine_threadsafe(
                self.task_queue.put((task_id, event.data)), 
                self.loop
            )

    async def process_queue(self):
        """Process tasks from the queue"""
        while not self._stop_event.is_set():
            try:
                task_id, task_data = await self.task_queue.get()
                if task_data.get('status') == 'pending':
                    await self.process_task(task_id, task_data)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing queue: {str(e)}")

    async def update_presence(self) -> None:
        """Update worker presence in Firebase"""
        presence_ref = db.reference(f'presence/{self.worker_id}')
        
        while not self._stop_event.is_set():
            try:
                presence_ref.set({
                    'status': 'busy' if self.processing else 'idle',
                    'lastSeen': datetime.now().isoformat(),
                    'type': 'worker'
                })
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating presence: {str(e)}")

    async def run(self) -> None:
        """Run the scheduler"""
        self.loop = asyncio.get_running_loop()
        
        try:
            logger.info(f"Starting DASH scheduler worker {self.worker_id}")
            
            # Start listening for new tasks
            tasks_ref = db.reference('tasks')
            tasks_ref.listen(self._task_callback)
            
            # Start workers
            queue_worker = asyncio.create_task(self.process_queue())
            presence_worker = asyncio.create_task(self.update_presence())
            
            # Wait for stop event
            await self._stop_event.wait()
            
            # Clean up
            queue_worker.cancel()
            presence_worker.cancel()
            await asyncio.gather(queue_worker, presence_worker, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
        finally:
            # Clean up presence
            try:
                db.reference(f'presence/{self.worker_id}').delete()
            except Exception as e:
                logger.error(f"Error cleaning up presence: {str(e)}")

    def stop(self):
        """Stop the scheduler"""
        self._stop_event.set()

if __name__ == "__main__":
    # Generate a unique worker ID
    worker_id = str(uuid.uuid4())
    
    # Create and run the scheduler
    scheduler = DashScheduler(
        cred_path='dash-b26cb-firebase-adminsdk-g5034-52af7a1672.json',
        worker_id=worker_id
    )
    
    try:
        asyncio.run(scheduler.run())
    except KeyboardInterrupt:
        logger.info("Scheduler shutting down...")
        scheduler.stop()
