# Start from a standard Python 3.10 image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the reaper's specific code
COPY reaper/reaper.py .

# The command to run when the container starts
CMD ["python", "reaper.py"]

