import firebase_admin
from firebase_admin import credentials, db
import logging
from datetime import datetime, timezone
import asyncio
import sys
import json
from typing import Dict, Any
from dataclasses import dataclass
import colorama
from colorama import Fore, Style

# Initialize colorama for cross-platform colored output
colorama.init()

@dataclass
class NodeMetrics:
    client_id: str
    cpu_cores: int
    cpu_frequency: float
    ram_gb: float
    has_docker: bool
    status: str
    last_seen: datetime

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
        self.node_metrics: Dict[str, NodeMetrics] = {}
        self.MAX_TASKS_PER_NODE = 3
        self.NODE_TIMEOUT_SECONDS = 300  # 5 minutes

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

    def _parse_cpu_info(self, cpu_str: str) -> tuple[int, float]:
        """Parse CPU cores and frequency from string"""
        try:
            parts = cpu_str.split('@')
            cores = int(parts[0].strip().split()[0])
            freq = float(parts[1].strip().split()[0])
            return cores, freq
        except Exception:
            return 1, 0.0

    def _parse_ram(self, ram_str: str) -> float:
        """Parse RAM from string"""
        try:
            return float(ram_str.split()[0])
        except Exception:
            return 0.0

    def _parse_iso_datetime(self, datetime_str: str) -> datetime:
        """Parse ISO datetime string and return UTC datetime object"""
        try:
            dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception as e:
            self.logger.error(f"Error parsing datetime {datetime_str}: {e}")
            return datetime.now(timezone.utc)

    def _get_current_time(self) -> datetime:
        """Get current time as UTC timezone-aware datetime"""
        return datetime.now(timezone.utc)

    def _update_node_metrics(self):
        """Update internal node metrics from Firebase presence data"""
        clients = self.clients_ref.get() or {}
        current_time = self._get_current_time()
        
        self.node_metrics.clear()
        for client_id, data in clients.items():
            if not isinstance(data, dict) or 'systemMetadata' not in data:
                continue

            metadata = data['systemMetadata']
            cores, freq = self._parse_cpu_info(metadata.get('cpu', '1 cores @ 0 MHz'))
            ram = self._parse_ram(metadata.get('ram', '0 GB'))
            
            last_seen = self._parse_iso_datetime(data['lastSeen'])
            if (current_time - last_seen).total_seconds() > self.NODE_TIMEOUT_SECONDS:
                continue

            self.node_metrics[client_id] = NodeMetrics(
                client_id=client_id,
                cpu_cores=cores,
                cpu_frequency=freq,
                ram_gb=ram,
                has_docker=metadata.get('docker', False),
                status=data.get('status', 'unknown'),
                last_seen=last_seen
            )

    def _get_node_load(self, client_id: str) -> int:
        """Get current task load for a node"""
        tasks = self.tasks_ref.get() or {}
        return sum(1 for task in tasks.values() 
                  if task.get('assignedTo') == client_id 
                  and task.get('status') == 'assigned')

    def _calculate_node_score(self, node: NodeMetrics, task: Dict[str, Any]) -> float:
        """Calculate a score for how well a node matches a task's requirements"""
        # First check if this node created the task
        creator_id = task.get('clientId')
        if creator_id and creator_id == node.client_id:
            return -1  
        
        if node.status != 'idle':
            return -1
        
        current_load = self._get_node_load(node.client_id)
        if current_load >= self.MAX_TASKS_PER_NODE:
            return -1
            
        # Base score starts with available resources
        score = (node.cpu_cores * node.cpu_frequency / 1000)  # CPU score
        score += node.ram_gb * 10  # RAM score
        
        # Reduce score based on current load
        score *= (1 - (current_load / self.MAX_TASKS_PER_NODE))

        # Task type specific requirements
        task_type = task.get('taskType', '')
        if task_type == 'docker' and not node.has_docker:
            return -1
        
        # Check docker-specific requirements
        if task_type == 'docker' and 'dockerConfig' in task:
            config = task['dockerConfig']
            required_cpu = float(config.get('cpuLimit', 0))
            required_memory = float(config.get('memoryLimit', '0').rstrip('m')) / 1024  # Convert to GB
            
            if required_cpu > node.cpu_cores or required_memory > node.ram_gb:
                return -1
            
            cpu_fit = 1 - (required_cpu / node.cpu_cores)
            memory_fit = 1 - (required_memory / node.ram_gb)
            score *= (cpu_fit + memory_fit) / 2

        return score

    async def update_presence(self):
        self.logger.info("Starting presence monitoring...")
        previous_clients = {}

        while True:
            try:
                clients = self.clients_ref.get() or {}
                active_clients = {cid: data for cid, data in clients.items() if data.get('status') == 'idle'}
                self._log_client_changes(previous_clients, active_clients)
                previous_clients = active_clients.copy()
                if self._get_current_time().second == 0:
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
        # Sort tasks by priority and age
        sorted_tasks = sorted(
            pending_tasks.items(),
            key=lambda x: (
                x[1].get('priority', 'normal'),
                x[1].get('createdAt', ''),
            ),
            reverse=True
        )

        for task_id, task in sorted_tasks:
            try:
                if 'clientId' not in task:
                    self.logger.warning(f"Task {task_id} missing clientId field, skipping...")
                    continue
                    
                self._update_node_metrics()
                # Filter out the creator node from available nodes
                available_nodes = [
                    node for node in self.node_metrics.values()
                    if node.client_id != task.get('clientId')
                ]
                
                if not available_nodes:
                    self.logger.warning(f"No available nodes for task {task_id} (excluding creator)")
                    continue
                    
                best_node = max(
                    available_nodes,
                    key=lambda node: self._calculate_node_score(node, task),
                    default=None
                )
                
                if best_node and self._calculate_node_score(best_node, task) > 0:
                    await self._assign_task(task_id, task, best_node.client_id)
                else:
                    self.logger.warning(f"No suitable node found for task {task_id}")
            except Exception as e:
                self.logger.error(f"Error processing task {task_id}: {str(e)}")

    async def _assign_task(self, task_id: str, task: Dict[str, Any], client: str):
        try:
            # Double-check we're not assigning to creator
            if client == task.get('clientId'):
                self.logger.error(f"Attempted to assign task {task_id} back to creator node {client}")
                return
                
            assignment_time = self._get_current_time().isoformat()
            
            update_data = {
                'status': 'assigned',
                'assignedTo': client,
                'assignedAt': assignment_time,
                'schedulerMetadata': {
                    'nodeMetrics': {
                        'cpuCores': self.node_metrics[client].cpu_cores,
                        'ramGb': self.node_metrics[client].ram_gb,
                        'currentLoad': self._get_node_load(client)
                    }
                }
            }
            
            self.tasks_ref.child(task_id).update(update_data)
            
            self.logger.info(
                f"Task Assignment Details:\n"
                f"  Task ID: {task_id}\n"
                f"  Created By: {task.get('clientId')}\n"
                f"  Assigned To: {client}\n"
                f"  Type: {task.get('taskType', 'unknown')}\n"
                f"  Priority: {task.get('priority', 'normal')}\n"
                f"  Node CPU: {self.node_metrics[client].cpu_cores} cores\n"
                f"  Node RAM: {self.node_metrics[client].ram_gb:.1f} GB\n"
                f"  Current Load: {self._get_node_load(client)} tasks"
            )
            
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