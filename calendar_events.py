import datetime
import os
from dotenv import load_dotenv
import mysql.connector
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

# Email of the service account
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL")

# If modifying these scopes, specify the required scopes for accessing Google Calendar.
SCOPES = ['https://www.googleapis.com/auth/calendar']

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
        print("Connected to MySQL database")
        return connection
    except mysql.connector.Error as e:
        print("Error connecting to MySQL:", e)
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

    cursor.close()
    mysql_connection.close()

    return calendar_id

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

    # Save calendar ID to the database
    try:
        cursor = mysql_connection.cursor()
        insert_query = "UPDATE User SET CalendarId = %s WHERE UserId = %s"
        cursor.execute(insert_query, (calendar_id, user_id))
        mysql_connection.commit()
        print("Calendar ID saved to the database for user:", user_id)
        cursor.close()
    except mysql.connector.Error as e:
        print("Error inserting calendar ID into database:", e)
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
            'summary': event_details[1],  # Assuming summary is at index 1 in the database
            'start': {
                'dateTime': event_details[2].isoformat()+ 'Z',  # Assuming start datetime is at index 2
            },
            'end': {
                'dateTime': event_details[3].isoformat()+ 'Z',  # Assuming end datetime is at index 3
            },
            'timeZone': 'Europe/Brussels',
            'location': event_details[4],  # Assuming location is at index 4
            'description': event_details[5]  # Assuming description is at index 5
        }

        # Insert event into Google Calendar
        created_event = service.events().insert(calendarId=calendar_id, body=event_body).execute()
        print('Event added to Google Calendar:', created_event['id'])
    else:
        print("Event with id", event_id, "not found in the database.")

    cursor.close()
    mysql_connection.close()


if __name__ == '__main__':
    user_id_from_rabbitmq = 26 # Replace with actual user ID received from RabbitMQ
    event_id = 6  # Replace with actual event ID received from RabbitMQ
    create_calendar(user_id_from_rabbitmq)
    add_event_to_calendar(user_id_from_rabbitmq, event_id)
    
