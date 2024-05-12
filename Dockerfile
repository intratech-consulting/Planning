# Use the official Python image as a base
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the Python script and requirements file into the container
COPY . .

RUN pip install --upgrade pip

# Install the required Python packages
RUN pip install --no-cache-dir \
    pika \
    google-api-python-client google-auth-httplib2 google-auth-oauthlib \
    lxml \
    mysql-connector-python \
    python-dotenv \
    python-dateutil \
    pytz \
    supervisor \
    sqlalchemy \
    pymysql \
    sys 

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf


# Run the Python script when the container starts
CMD [ "supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
