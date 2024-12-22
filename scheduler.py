import firebase_admin
from firebase_admin import credentials, db
import logging
from datetime import datetime
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashScheduler:
    def __init__(self, cred_path: str):
        """Initialize the DASH scheduler"""
        try:
            firebase_admin.get_app()
        except ValueError:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {
                'databaseURL': "https://dash-b26cb-default-rtdb.firebaseio.com"
            })
        
        self.tasks_ref = db.reference('tasks')
        self.clients_ref = db.reference('presence')

    async def update_presence(self):
        """Monitor and log active clients"""
        while True:
            try:
                clients = self.clients_ref.get() or {}
                active_clients = {
                    cid: data 
                    for cid, data in clients.items() 
                    if data.get('status') == 'idle'
                }
                logger.info(f"Active Clients: {active_clients}")
            except Exception as e:
                logger.error(f"Error updating presence: {str(e)}")
            
            await asyncio.sleep(30)  # Check every 30 seconds

    async def process_tasks(self):
        """Check and process pending tasks periodically"""
        while True:
            try:
                # Get all pending tasks
                tasks = self.tasks_ref.get() or {}
                pending_tasks = {
                    task_id: task 
                    for task_id, task in tasks.items() 
                    if task.get('status') == 'pending'
                }

                # Process each pending task
                for task_id, task in pending_tasks.items():
                    clients = self.clients_ref.get() or {}
                    active_clients = [
                        cid 
                        for cid, data in clients.items() 
                        if data.get('status') == 'idle'
                    ]

                    if active_clients:
                        assigned_client = active_clients[0]
                        self.tasks_ref.child(task_id).update({
                            'status': 'assigned',
                            'assignedTo': assigned_client,
                            'assignedAt': datetime.now().isoformat()
                        })
                        logger.info(f"Assigned task {task_id} to client {assigned_client}")
                    else:
                        logger.warning("No active clients available")

            except Exception as e:
                logger.error(f"Error processing tasks: {str(e)}")

            await asyncio.sleep(5)  # Check every 5 seconds

    async def run(self):
        """Run the scheduler"""
        logger.info("Starting DASH scheduler")
        await asyncio.gather(
            self.update_presence(),
            self.process_tasks()
        )

if __name__ == "__main__":
    scheduler = DashScheduler(
        cred_path='dash-b26cb-firebase-adminsdk-g5034-52af7a1672.json'
    )
    
    try:
        asyncio.run(scheduler.run())
    except KeyboardInterrupt:
        logger.info("Scheduler shutting down...")