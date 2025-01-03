# Use the official Python image as base
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files into the container
COPY . .

# Command to run the scheduler
CMD ["python", "scheduler.py"]
