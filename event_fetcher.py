import datetime
import os
import time
import mysql.connector
import logging
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(filename='event_fetcher.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# If modifying these scopes, specify the required scopes for accessing Google Calendar.
SCOPES = ['https://www.googleapis.com/auth/calendar']

# Load environment variables from .env file
load_dotenv()

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

def fetch_events(calendar_service, start_date, end_date, mysql_connection, interval_seconds=3600):
    try:
        while True:
            # Fetch events from Google Calendar
            events_result = calendar_service.events().list(
                calendarId= "9ecbb3026111b91a9ce21bfed88d67b95783a5a418c6d82aaa220776eb70f5d3@group.calendar.google.com",
                timeMin=start_date.isoformat() + 'Z',
                timeMax=end_date.isoformat() + 'Z',
                singleEvents=True,
            ).execute()

            events = events_result.get('items', [])

            if events:
                try:
                    cursor = mysql_connection.cursor()
                    insert_query = "INSERT INTO Events (summary, start_datetime, end_datetime, location, description) VALUES (%s, %s, %s, %s, %s)"
                    for event in events:
                        summary = event['summary']
                        start = event['start'].get('dateTime', event['start'].get('date'))
                        end = event['end'].get('dateTime', event['end'].get('date'))
                        location = event.get('location', 'N/A')
                        description = event.get('description', 'N/A')

                        # Check if event already exists in the database
                        event_exists_query = "SELECT COUNT(*) FROM Events WHERE summary = %s AND start_datetime = %s AND end_datetime = %s"
                        cursor.execute(event_exists_query, (summary, start, end))
                        event_count = cursor.fetchone()[0]

                        if event_count == 0:
                            # Insert event into MySQL table if it doesn't exist
                            cursor.execute(insert_query, (summary, start, end, location, description))
                            logging.info("Event inserted into MySQL table: %s", summary)
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

def start_event_fetching(calendar_service, interval_seconds=3600):
    mysql_connection = connect_to_mysql()
    if mysql_connection is None:
        return

    # Set start date to today and end date to 3 weeks before
    start_date = datetime.datetime.now()
    end_date = start_date - datetime.timedelta(weeks=3)

    fetch_events(calendar_service, start_date, end_date, mysql_connection, interval_seconds)

if __name__ == "__main__":
    start_event_fetching(service)
