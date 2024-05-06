# Use the official Python image as a base
# Use a specific Python version
FROM python:3.9

# Set the working directory inside the container
WORKDIR /TestServer/planning/github/Planning

# Copy the Python script and requirements file into the container
COPY . .

# Install the required Python packages
RUN pip install --no-cache-dir \
    pika \
    google-api-python-client google-auth-httplib2 google-auth-oauthlib \
    lxml \
    mysql-connector-python \
    python-dotenv \
    python-dateutil \
    pytz

# Run the Python script when the container starts
CMD [ "python3", "heartbeat_planning.py" ]