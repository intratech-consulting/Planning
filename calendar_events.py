import datetime
import os
import publisher_planning
from dotenv import load_dotenv
import mysql.connector
from googleapiclient.discovery import build
from google.oauth2 import service_account
import googleapiclient.errors
import sys
import logging


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

# Load environment variables from .env file
load_dotenv()

# Email of the service account
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL")

# If modifying these scopes, specify the required scopes for accessing Google Calendar.
SCOPES = ['https://www.googleapis.com/auth/calendar']


def connect_to_mysql():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 3306),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        print("Connected to MySQL database")
        return connection
    except mysql.connector.Error as e:
        print("Error connecting from calendar.py to MySQL:", e)
        return None

def create_calendar(user_id):
    # Create a new calendar, add events to it, and save calendar link to the user.
    # Authenticate using service account credentials
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

    # MySQL Connection
    mysql_connection = connect_to_mysql()
    if mysql_connection is None:
        return None

    # Check if the user has a calendar ID in the database
    cursor = mysql_connection.cursor()
    select_query = "SELECT CalendarId FROM User WHERE UserId = %s"
    cursor.execute(select_query, (user_id,))
    result = cursor.fetchone()
    if result and result[0]:
        calendar_id = result[0]
        print("User has an existing calendar. Using calendar ID:", calendar_id)
    else:
        # Create a new calendar
        calendar_id = create_new_calendar(service, mysql_connection, user_id)
        return calendar_id

    cursor.close()
    mysql_connection.close()
    

def create_new_calendar(service, mysql_connection, user_id):
    # Create a new calendar
    calendar = {
        'summary': 'Integration Project 5',
        'timeZone': 'Europe/Brussels',
        'defaultReminders': [],
        'accessRole': 'owner',
        'role': 'owner'
    }

    created_calendar = service.calendars().insert(body=calendar).execute()
    print('Calendar created:', created_calendar['id'])
    calendar_id = created_calendar['id']
    calendar_link = f"https://calendar.google.com/calendar/embed?src={calendar_id}"
    print('Calendar link:', calendar_link)

    # Share the calendar publicly
    rule = {
        'scope': {
            'type': 'default',
        },
        'role': 'reader'  # Allow anyone to view the calendar
    }
    service.acl().insert(calendarId=calendar_id, body=rule).execute()
    print('Calendar shared publicly')

    # Add service account as an owner of the calendar
    rule = {
        'scope': {
            'type': 'user',
            'value': SERVICE_ACCOUNT_EMAIL,
        },
        'role': 'owner'  # Service account has ownership access
    }
    service.acl().insert(calendarId=calendar_id, body=rule).execute()
    print('Permissions granted for service account:', SERVICE_ACCOUNT_EMAIL)

    # Save calendar ID and link to the database
    try:
        cursor = mysql_connection.cursor()
        logger.debug(calendar_id)
        logger.debug(calendar_link)
        update_query = "UPDATE User SET CalendarId = %s, CalendarLink = %s WHERE UserId = %s"
        logger.debug(update_query)
        cursor.execute(update_query, (calendar_id, calendar_link, user_id))
        mysql_connection.commit()
        logger.debug(f"Calendar ID and link saved to the database for user:{user_id}")
        publisher_planning.publish_user_xml(user_id)
        cursor.close()
    except mysql.connector.Error as e:
        logger.error(f"Error updating database:{e}")
        mysql_connection.rollback()

    return calendar_id

def add_event_to_calendar(user_id, event_id):
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

    # MySQL Connection
    mysql_connection = connect_to_mysql()
    if mysql_connection is None:
        return None

    # Fetch calendar_id associated with user_id
    cursor = mysql_connection.cursor()
    select_query = "SELECT CalendarId FROM User WHERE UserId = %s"
    cursor.execute(select_query, (user_id,))
    result = cursor.fetchone()
    if result and result[0]:
        calendar_id = result[0]
    else:
        print("Calendar ID not found for user with ID:", user_id)
        cursor.close()
        mysql_connection.close()
        return

    # Fetch event details from the database
    select_query = "SELECT * FROM Events WHERE Id = %s"
    cursor.execute(select_query, (event_id,))
    event_details = cursor.fetchone()

    if event_details:
        # Construct event body
        event_body = {
            'summary': event_details[2],  # Assuming summary is at index 1 in the database
            'start': {
                'dateTime': event_details[3].isoformat()+ 'Z',  # Assuming start datetime is at index 2
            },
            'end': {
                'dateTime': event_details[4].isoformat()+ 'Z',  # Assuming end datetime is at index 3
            },
            'timeZone': 'Europe/Brussels',
            'location': event_details[5],  # Assuming location is at index 4
            'description': event_details[6]  # Assuming description is at index 5
        }

        # Insert event into Google Calendar
        created_event = service.events().insert(calendarId=calendar_id, body=event_body).execute()
        print('Event added to Google Calendar:', created_event['id'])
    else:
        print("Event with id", event_id, "not found in the database.")

    cursor.close()
    mysql_connection.close()


def delete_event_by_summary(user_id, event_summary):
    # Authenticate using service account credentials
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

    # MySQL Connection
    mysql_connection = connect_to_mysql()
    if mysql_connection is None:
        return None

    # Fetch calendar_id associated with user_id
    cursor = mysql_connection.cursor()
    select_query = "SELECT CalendarId FROM User WHERE UserId = %s"
    cursor.execute(select_query, (user_id,))
    result = cursor.fetchone()
    if result and result[0]:
        calendar_id = result[0]
    else:
        logger.error(f"Calendar ID not found for user with ID: {user_id}")
        cursor.close()
        mysql_connection.close()
        return

    cursor.close()
    mysql_connection.close()

    # List all events in the calendar
    events_result = service.events().list(calendarId=calendar_id).execute()
    events = events_result.get('items', [])

    # Find and delete the event with the matching summary
    event_to_delete = None
    for event in events:
        if event['summary'] == event_summary:
            event_to_delete = event['id']
            break

    if event_to_delete:
        service.events().delete(calendarId=calendar_id, eventId=event_to_delete).execute()
        logger.info(f"Event with summary '{event_summary}' has been deleted from Google Calendar.")
    else:
        logger.warning(f"No event found with summary '{event_summary}' in Google Calendar.")


    cursor.close()
    mysql_connection.close()


#if __name__ == '__main__':
 #user_id_from_rabbitmq = 'dab9414f-5530-4ddc-920a-1fd74a31c415' # Hardcoded, make it a comment when we use function calls
    #  event_id = 7  # Hardcoded, make it a comment when we use function calls
    #create_calendar(user_id_from_rabbitmq)
        #add_event_to_calendar(user_id_from_rabbitmq, event_id)
    
