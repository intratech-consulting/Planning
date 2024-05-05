from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import os.path
from dotenv import load_dotenv
import mysql.connector

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

    # MySQL Connection
    mysql_connection = connect_to_mysql()
    if mysql_connection is None:
        return None, None

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

    # Define start and end dates (assuming they are defined elsewhere in your code)
    start_date = datetime.datetime(2024, 5, 1)  # Example start date
    end_date = datetime.datetime(2024, 5, 31)   # Example end date

    # Fetch events from the public calendar within the time range
    public_calendar_events = fetch_events(service, start_date, end_date)

    if public_calendar_events is not None:
        if public_calendar_events:
            for event in public_calendar_events:
                summary = event['summary']
                start = event['start'].get('dateTime', event['start'].get('date'))
                end = event['end'].get('dateTime', event['end'].get('date'))
                location = event.get('location', 'N/A')
                description = event.get('description', 'N/A')

                # Create event
                event_body = {
                    'summary': summary,
                    'location': location,
                    'description': description,
                    'start': {
                        'dateTime': start,
                        'timeZone': 'Europe/Brussels',
                    },
                    'end': {
                        'dateTime': end,
                        'timeZone': 'Europe/Brussels',
                    },
                    'reminders': {
                        'useDefault': True,
                    },
                }

                # Add event to the newly created calendar
                service.events().insert(calendarId=calendar_id, body=event_body).execute()
                print('Event created for calendar:', calendar_id)
        else:
            print("No events found in the public calendar within the specified time range.")
    else:
        print("Failed to fetch events from the public calendar.")

    # Insert calendar link into the USER table
    try:
        cursor = mysql_connection.cursor()
        insert_query = "INSERT INTO USER (firstname, lastname, email, status, calendarlink) VALUES (%s, %s, %s, %s, %s)"
        calendar_link = f"https://calendar.google.com/calendar/embed?src={calendar_id}&ctz=Europe%2FBrussels"
        cursor.execute(insert_query, ('John', 'Doe', 'john.doe@example.com', 1, calendar_link))
        mysql_connection.commit()
        print("Calendar link inserted into USER table")
    except mysql.connector.Error as e:
        print("Error inserting calendar link into USER table:", e)
        mysql_connection.rollback()

    return calendar_id, f"https://calendar.google.com/calendar/embed?src={calendar_id}&ctz=Europe%2FBrussels"  # Return the ID of the newly created calendar and its embed link

def fetch_events(calendar_service, start_date, end_date):
    try:
       # Fetch events from Google Calendar
        events_result = calendar_service.events().list(
        calendarId='9ecbb3026111b91a9ce21bfed88d67b95783a5a418c6d82aaa220776eb70f5d3@group.calendar.google.com',
        timeMin=start_date.isoformat() + 'Z',
        timeMax=end_date.isoformat() + 'Z',
        singleEvents=True,
        ).execute()

        events = events_result.get('items', [])
        
        return events
    except Exception as e:
        print("An error occurred:", e)
        return None

if __name__ == '__main__':
    calendar_id, embed_link = create_calendar()
    print('Calendar ID:', calendar_id)
    print('Embed Link:', embed_link)
