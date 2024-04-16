from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import datetime
import pickle
import os.path
import xml.etree.ElementTree as ET

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar']

def create_calendar():
    """Create a new calendar and add events to it."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('calendar', 'v3', credentials=creds)

    # Create a new calendar
    calendar = {
        'summary': 'Integration Project',
        'timeZone': 'Europe/Brussels'
    }

    created_calendar = service.calendars().insert(body=calendar).execute()
    print('Calendar created:', created_calendar['id'])

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

        # Add event to the newly created calendar
        created_event = service.events().insert(calendarId=created_calendar['id'], body=event_body).execute()
        print('Event created:', created_event['id'])

if __name__ == '__main__':
    create_calendar()

'''
To Do:
- Modify the code so we can check if a calendar already exists
python -m venv venv 
pip install google-api
pip install google-auth-oauthlib
'''