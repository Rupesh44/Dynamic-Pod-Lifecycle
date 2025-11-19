# Start from a standard Python 3.10 image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file *first* and install dependencies
# This layer is cached and only rebuilds if requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the gateway's specific code
COPY gateway/gateway.py .

# The command to run when the container starts
# We use Uvicorn to run our FastAPI app
CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "80"]
