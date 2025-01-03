import firebase_admin
from firebase_admin import credentials, db
import logging
from datetime import datetime
import asyncio
import sys
import json
from typing import Dict, Any
import colorama
from colorama import Fore, Style

# Initialize colorama for cross-platform colored output
colorama.init()

# Configure logging with custom formatter
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT
    }

    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
        timestamp = self.formatTime(record, self.datefmt)
        return f"{timestamp} | {record.levelname:<20} | {record.threadName:<15} | {record.getMessage()}"

class DashScheduler:
    def __init__(self, cred_path: str):
        self._setup_logging()
        self.logger.info("Initializing DASH Scheduler...")

        try:
            self._initialize_firebase(cred_path)
            self.tasks_ref = db.reference('tasks')
            self.clients_ref = db.reference('presence')
            self.logger.info("Successfully connected to Firebase")
        except Exception as e:
            self.logger.critical(f"Failed to initialize Firebase: {e}")
            raise

    def _setup_logging(self):
        self.logger = logging.getLogger('DashScheduler')
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColoredFormatter('%(asctime)s.%(msecs)03d'))
        file_handler = logging.FileHandler('dashscheduler.log')
        file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(threadName)-15s | %(message)s'))
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def _initialize_firebase(self, cred_path: str):
        try:
            firebase_admin.get_app()
            self.logger.debug("Using existing Firebase app")
        except ValueError:
            try:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred, {'databaseURL': "https://dash-b26cb-default-rtdb.firebaseio.com"})
                self.logger.debug("Created new Firebase app")
            except Exception as e:
                self.logger.error(f"Failed to initialize Firebase: {str(e)}")
                raise

    async def update_presence(self):
        self.logger.info("Starting presence monitoring...")
        previous_clients = {}

        while True:
            try:
                clients = self.clients_ref.get() or {}
                active_clients = {cid: data for cid, data in clients.items() if data.get('status') == 'idle'}
                self._log_client_changes(previous_clients, active_clients)
                previous_clients = active_clients.copy()
                if datetime.now().second == 0:
                    self._log_detailed_status(clients)
            except Exception as e:
                self.logger.error(f"Error updating presence: {str(e)}", exc_info=True)
            await asyncio.sleep(30)

    def _log_client_changes(self, previous: Dict[str, Any], current: Dict[str, Any]):
        new_clients = set(current.keys()) - set(previous.keys())
        disconnected_clients = set(previous.keys()) - set(current.keys())
        for client in new_clients:
            self.logger.info(f"New client connected: {client}")
        for client in disconnected_clients:
            self.logger.warning(f"Client disconnected: {client}")

    def _log_detailed_status(self, clients: Dict[str, Any]):
        total_clients = len(clients)
        active_count = sum(1 for c in clients.values() if c.get('status') == 'idle')
        busy_count = sum(1 for c in clients.values() if c.get('status') == 'busy')
        status_msg = f"\nSystem Status Summary:\nTotal Clients: {total_clients}\nActive Clients: {active_count}\nBusy Clients: {busy_count}\n"
        self.logger.info(status_msg)

    async def process_tasks(self):
        self.logger.info("Starting task processor...")

        while True:
            try:
                tasks = self.tasks_ref.get() or {}
                pending_tasks = {task_id: task for task_id, task in tasks.items() if task.get('status') == 'pending'}
                if pending_tasks:
                    self.logger.debug(f"Found {len(pending_tasks)} pending tasks")
                    await self._process_pending_tasks(pending_tasks)
                else:
                    self.logger.debug("No pending tasks")
            except Exception as e:
                self.logger.error(f"Error processing tasks: {str(e)}", exc_info=True)
            await asyncio.sleep(5)

    async def _process_pending_tasks(self, pending_tasks: Dict[str, Any]):
        for task_id, task in pending_tasks.items():
            try:
                clients = self.clients_ref.get() or {}
                active_clients = [cid for cid, data in clients.items() if data.get('status') == 'idle']
                # if task.get('clientId') in active_clients:
                #     active_clients.remove(task['clientId'])
                if active_clients:
                    assigned_client = active_clients[0]
                    await self._assign_task(task_id, task, assigned_client)
                else:
                    self.logger.warning(f"No eligible clients available for task {task_id}")
            except Exception as e:
                self.logger.error(f"Error processing task {task_id}: {str(e)}")

    async def _assign_task(self, task_id: str, task: Dict[str, Any], client: str):
        try:
            assignment_time = datetime.now().isoformat()
            self.tasks_ref.child(task_id).update({'status': 'assigned', 'assignedTo': client, 'assignedAt': assignment_time})
            self.logger.info(f"Task Assignment: ID={task_id} Client={client} Type={task.get('type', 'unknown')} Priority={task.get('priority', 'normal')}")
        except Exception as e:
            self.logger.error(f"Failed to assign task {task_id} to client {client}: {str(e)}")
            raise

    async def run(self):
        self.logger.info("Starting DASH scheduler...")
        try:
            await asyncio.gather(self.update_presence(), self.process_tasks())
        except asyncio.CancelledError:
            self.logger.info("Received shutdown signal...")
        except Exception as e:
            self.logger.critical(f"Critical error in scheduler: {str(e)}", exc_info=True)
            raise
        finally:
            self.logger.info("Cleaning up resources... Scheduler shutdown complete")

if __name__ == "__main__":
    try:
        scheduler = DashScheduler(cred_path='dash-b26cb-firebase-adminsdk-g5034-52af7a1672.json')
        asyncio.run(scheduler.run())
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt, shutting down...")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)
