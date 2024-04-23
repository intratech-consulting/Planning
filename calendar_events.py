from googleapiclient.discovery import build
from google.oauth2 import service_account
import datetime
import os.path
import xml.etree.ElementTree as ET

# Email of the service account
SERVICE_ACCOUNT_EMAIL = 'google-calendar@integration-project-420318.iam.gserviceaccount.com'

# If modifying these scopes, specify the required scopes for accessing Google Calendar.
SCOPES = ['https://www.googleapis.com/auth/calendar']

def create_calendar():
    """Create a new calendar and add events to it."""
    # Authenticate using service account credentials
    creds = service_account.Credentials.from_service_account_info(
        {
            "type": "service_account",
            "project_id": "integration-project-420318",
            "private_key_id": "d60076a099a514387563237ae1f4b8f6754dca0b",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC4cixiVVFjK4yE\nASdCVu12zM9m6I6XadAl8Gj0Wwt2hGUL0nidvuEEn3UFCdMvv3DqjcztCvqVR6K6\nE4WA8LOS3YR2GBS2/gwpYguk3/DUotCt70QcnML4bgnklsMYky03qbw7PihNIah+\n6BDdE/vznGAGXtW6Z+LBkOf/gEx198OgvsPjtuv+JBhNSG4yqPxb29Xl5wWlts0q\n01FMxa3ONRQp+dEe+FdHI60n3a/GZxG6PAjpHGnjPIPsF7iUcgETMmWT4VkhaICJ\ncDd++74BAr0t28NAwvbe31klLosBWh0tvfCaV4GTSCvVvFExbcANQ5Yoz9Mzq0OB\n5guNr67vAgMBAAECggEABMGc92Qv1xjWZMkK1CEduoSJjKPOmYT3AyNx8WsvnnzH\nhkiFJ7w2XUInaFvagE61gLbrY+l2kMbqIVZDiRGr78uakNkKH7XpZl/K4RONSE5y\nhym2Xe6E8JcV1jk1Crsw3VCZNAXkUCjgCXW+iZTsWFnd+AwWAtqO9GLiOKp8TkZs\nGDrnz1PnR/pLificKT9niJcQOi0ryYHfkFV5V6uLMr0DIXWm+/411XX8XHm49JYp\nzKgz7zJR0qHmLo4UVUROOocYCVRoKcG/WOTZl9aSOoKxtdbQIHbFl+wiRjVBH8I7\negatFJ97UvcijVHdEJQL4M+9EnqVBW4PZp+V5wJDcQKBgQDvrLrtY2zgEInxIpoW\nm5Uz5ReucFgOaz2tvLkZQRdu2Zfz3HQnBqWcACTKx1VskVqzLOCt0HxDIM0tkgZo\niDhvOoCyma4X/Rkzug1b8x9FX1G3f2VD3lc7MYfQhTA2+rPm2ZeRACX0cvB/cQeG\nmZTK1nsROLcrwPywM7g3dvMc/QKBgQDFAmfO5E8ayoXtkHPZ4kbdXnBp7Bs9twph\ncZSpyOVAuwUPkZG3LQvdj7uniIKXr5O7vTlVu+G9L4rIf5hMNVScBVToiW62Dx3A\nI+G5NTCvABMltt3Pxyt/SH9+k1cyPn27SdN/hOa6gJ0fwHCRRF2Vpu+eWny0xr2Y\n9UNCRpQ1WwKBgBIHimBFNHF+jQHounKs7YJj324t4VHkW1VwdvQhY2PhVn7vvIUb\nCOY9VIc0i4aJQKtt2tD4eACMvSKKc5T1ellTq07EJMaFMZokitC570ZGr10AZlu+\npA094pubCU95eIMLJ02vFyvWYaUUmZ5Ue07u538aJx1RfTC8E0WI2yz5AoGATexv\npvPDHjOw2ZBjYnEP2ot6J0tOUxDNOFG2nZlqpDnU/J11Z2CX6uDpNpDWJq053+4u\neWzBExV07W0ajjoh5zxsozWI5tX/zHnk1F7njLCY4jf8cCy1VbkVRLl+qc7x6FyB\nHVUx9d7dRyvU2oCSC5ll+dqMrOxXHeMU4WgNQzUCgYEAo2ISUJqFbwOPy5o/2x5K\nKLdS0iz9qhwzjm0uF2GvpNJC15LzH/e1S/hu9hghS1v76M7IhWml0WcgTOExTxb1\nx6fMlUcfNR5MmSVn1itZizAZieldGZ7DknotfiWXbQ6WpZrJloZ9WG2Z9OG+8XoO\nodPZBjIQ/yw3Y/qknZFdVj4=\n-----END PRIVATE KEY-----\n",
            "client_email": "caledars@integration-project-420318.iam.gserviceaccount.com",
            "client_id": "110225367326596907132",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/caledars%40integration-project-420318.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
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

    # Add service account as an owner of the calendar
    rule = {
        'scope': {
            'type': 'user',
            'value': SERVICE_ACCOUNT_EMAIL,
        },
        'role': 'owner'  # or 'writer', 'reader' as needed
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

    return created_calendar['id'], f"https://calendar.google.com/calendar/embed?src={created_calendar['id']}"  # Return the ID of the newly created calendar and its embed link

if __name__ == '__main__':
    calendar_id, embed_link = create_calendar()
    print('Calendar ID:', calendar_id)
    print('Embed Link:', embed_link)
