import datetime
import os
import time
import mysql.connector
import logging
import publisher_planning
import sys
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Create a custom logger
logger = logging.getLogger(__name__)

# Set the level of this logger.
# DEBUG, INFO, WARNING, ERROR, CRITICAL can be used depending on the granularity of log you want.
logger.setLevel(logging.DEBUG)

# Create handlers
c_handler = logging.StreamHandler()
s_handler = logging.StreamHandler(sys.stdout)
c_handler.setLevel(logging.DEBUG)
s_handler.setLevel(logging.DEBUG)  # Set level to DEBUG

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
s_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
s_handler.setFormatter(s_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(s_handler)

logger.debug('This is a debug message')
logger.info('This is an info message')

logger.warning('This is a warning')
logger.error('This is an error')

# If modifying these scopes, specify the required scopes for accessing Google Calendar.
SCOPES = ['https://www.googleapis.com/auth/calendar']

# Load environment variables from .env file
load_dotenv()

# MySQL Database Connection
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")


def connect_to_mysql():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        logging.info("Connected to MySQL database")
        return connection
    except mysql.connector.Error as e:
        logging.error("Error connecting to MySQL: %s", e)
        return None

def fetch_events(calendar_service, start_date, end_date, mysql_connection, interval_seconds=3):
    try:
        while True:
            # Fetch events from Google Calendar
            events_result = calendar_service.events().list(
                calendarId="9ecbb3026111b91a9ce21bfed88d67b95783a5a418c6d82aaa220776eb70f5d3@group.calendar.google.com",
                timeMin=start_date.isoformat() + 'Z',
                timeMax=end_date.isoformat() + 'Z',
                singleEvents=True,
            ).execute()

            events = events_result.get('items', [])

            if events:
                try:
                    cursor = mysql_connection.cursor()
                    insert_query = "INSERT INTO Events (summary, start_datetime, end_datetime, location, description, max_registrations, available_seats) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    for event in events:
                        summary = event['summary']
                        start = event['start'].get('dateTime', event['start'].get('date'))
                        end = event['end'].get('dateTime', event['end'].get('date'))
                        location_with_max = event.get('location', 'N/A')
                        # Split location to extract location and max_registrations
                        location_parts = location_with_max.split('-')
                        location = location_parts[0].strip() if len(location_parts) >= 1 else 'N/A'  # Extract location
                        max_registrations = int(location_parts[1]) if len(location_parts) >= 2 else 0  # Extract max_registrations
                        description = event.get('description', 'N/A')
                        available_seats = 50
                        # Check if event already exists in the database
                        event_exists_query = "SELECT COUNT(*) FROM Events WHERE summary = %s AND start_datetime = %s AND end_datetime = %s"
                        cursor.execute(event_exists_query, (summary, start, end))
                        event_count = cursor.fetchone()[0]

                        if event_count == 0:
                            # Insert event into MySQL table if it doesn't exist
                            cursor.execute(insert_query, (summary, start, end, location, description, max_registrations, available_seats))

                            retrieve_event_id_query = "SELECT Id FROM Events WHERE summary = %s"
                            # Check if the event exists in the MySQL table
                            cursor.execute(retrieve_event_id_query, (summary,))
                            event_id = cursor.fetchone()[0]

                            logging.info("Event inserted into MySQL table: %s", summary)
                            publisher_planning.publish_event_xml(event_id)
                        else:
                            logging.info("Event already exists in MySQL table. Skipping insertion: %s", summary)

                    mysql_connection.commit()
                    logging.info("Events inserted into MySQL table")
                except mysql.connector.Error as e:
                    logging.error("Error inserting events into MySQL table: %s", e)
                    mysql_connection.rollback()
                finally:
                    cursor.close()
            else:
                logging.info("No events found in the calendar within the specified time range.")

            time.sleep(interval_seconds)  # Sleep for specified interval before checking again

    except Exception as e:
        logging.error("An error occurred: %s", e)

if __name__ == "__main__":
    # Set start date to today and end date to 3 weeks after
    start_date = datetime.datetime.now()
    end_date = datetime.datetime.now() + datetime.timedelta(weeks=3)

   
    creds = service_account.Credentials.from_service_account_info(
        {
            "type": "service_account",
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("UNIVERSE_DOMAIN")
        },
        scopes=SCOPES
    )

    service = build('calendar', 'v3', credentials=creds)

    # Connect to MySQL
    mysql_connection = connect_to_mysql()
    if mysql_connection is None:
        exit()

    # Fetch events
    fetch_events(service, start_date, end_date, mysql_connection)
