# Start from a standard Python 3.10 image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the worker's specific code
COPY worker/worker.py .

# The command to run when the container starts
# We just run the Python script
CMD ["python", "worker.py"]
