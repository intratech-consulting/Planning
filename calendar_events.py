from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import os.path
import xml.etree.ElementTree as ET
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MySQL credentials
mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")

# Establish a connection to your MySQL database
mydb = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)

# Email of the service account
SERVICE_ACCOUNT_EMAIL = os.getenv("SERVICE_ACCOUNT_EMAIL")

# If modifying these scopes, specify the required scopes for accessing Google Calendar.
SCOPES = ['https://www.googleapis.com/auth/calendar']

def create_calendar():
    """Create a new calendar and add events to it."""
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

    # Check if the calendar already exists
    calendar_list = service.calendarList().list().execute()
    for calendar_entry in calendar_list['items']:
        if calendar_entry['summary'] == 'Integration Project 7':
            print('Calendar already exists:', calendar_entry['id'])
            return calendar_entry['id'], f"https://calendar.google.com/calendar/embed?src={calendar_entry['id']}"  # Return the existing calendar ID and embed link

    # Create a new calendar
    calendar = {
        'summary': 'Integration Project 5',
        'timeZone': 'Europe/Brussels'
    }

    created_calendar = service.calendars().insert(body=calendar).execute()
    print('Calendar created:', created_calendar['id'])

    # Save the calendarID to MySQL database
    mycursor = mydb.cursor()
    sql = "INSERT INTO calendar_table (calendar_id) VALUES (%s)"
    val = (created_calendar['id'],)
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")

    # Share the calendar publicly
    rule = {
        'scope': {
            'type': 'default',
        },
        'role': 'reader'  # Allow anyone to view the calendar
    }
    service.acl().insert(calendarId=created_calendar['id'], body=rule).execute()
    print('Calendar shared publicly')

    # Add service account as an owner of the calendar
    rule = {
        'scope': {
            'type': 'user',
            'value': SERVICE_ACCOUNT_EMAIL,
        },
        'role': 'owner'  # Service account has ownership access
    }
    service.acl().insert(calendarId=created_calendar['id'], body=rule).execute()
    print('Permissions granted for service account:', SERVICE_ACCOUNT_EMAIL)

    # Parse XML file
    tree = ET.parse('events.xml')
    root = tree.getroot()

    for event in root.findall('event'):
        # Extract event details
        date = event.find('date').text
        start_time = event.find('start_time').text
        end_time = event.find('end_time').text
        location = event.find('location').text
        speaker_name = event.find('speaker/name').text
        speaker_email = event.find('speaker/email').text
        speaker_company = event.find('speaker/company').text
        description = event.find('description').text

        # Create event
        event_body = {
            'summary': description,
            'location': location,
            'description': f'Speaker: {speaker_name} ({speaker_company})\nEmail: {speaker_email}',
            'start': {
                'dateTime': f'{date}T{start_time}:00',
                'timeZone': 'Europe/Brussels',
            },
            'end': {
                'dateTime': f'{date}T{end_time}:00',
                'timeZone': 'Europe/Brussels',
            },
            'reminders': {
                'useDefault': True,
            },
        }

        # Add event to the newly created or existing calendar
        service.events().insert(calendarId=created_calendar['id'], body=event_body).execute()
        print('Event created for calendar:', created_calendar['id'])

    return created_calendar['id'], f"https://calendar.google.com/calendar/embed?src={created_calendar['id']}&ctz=Europe%2FBrussels"  # Return the ID of the newly created calendar and its embed link

if __name__ == '__main__':
    calendar_id, embed_link = create_calendar()
    print('Calendar ID:', calendar_id)
    print('Embed Link:', embed_link)
