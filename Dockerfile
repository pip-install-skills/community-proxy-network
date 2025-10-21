# Use the official Python Alpine image from the Docker Hub
FROM python:3.11-alpine

# Set the working directory inside the container
WORKDIR /app

# Install dependencies required for building certain Python packages (e.g., build tools, headers)
RUN apk add --no-cache --virtual .build-deps gcc musl-dev libffi-dev

# Copy the requirements file first
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Remove build dependencies to keep the image smaller
RUN apk del .build-deps

# Copy the entire application code into the container
COPY . .

# Expose the port that the app runs on
EXPOSE 8000

# Command to run the FastAPI application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
