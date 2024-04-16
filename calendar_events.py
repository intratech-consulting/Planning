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
            "private_key_id": "6f5443266f07048b2dc13849a12a0be2129b5d2e",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDA5+8ACol7wPEn\n2SdndbCYb+9CziD2Jjm0sXFIg3p+OzyhXF9jextZ+VISWZayt6PlxwtLRll0ZPKh\n5HKEZJXxCdh8l5Y1lOuqtu8Y+saoCcRDRA0HWn+2I7930a+j+bpSX7moSrABS4AI\nqirwRpxlqC2y5GKt+PxU2i23/So1cAbGliagMPWB/2tAswqP7J3EIlmfKo9HMNz2\nbFRyZkJ5C558uc+L0ZG2PaQwfO8nFVPbcYvPP7G1AIrOi9V0xlQ4XPRmUwERjKB0\nlOg7V9LluSpuR10kb38iytKOQeO9VvJP8H8iedATVi0XIlViEfdtm1ArfIx1TQ3j\n9+4GMsBtAgMBAAECggEAAT9+5FktGofdB4z4HEs+oT3YJm5++Bi3NtwPB5ESMc7g\nTYrzcwlQ2/wo88VwnCdkABCRmiMSYRZmG3YG56Re8/hEVkoExsP34lrcoSS+NEgb\n5PODwhBr0xnYG0ZYBI+RDQdhhB03lkqOwX+YV4WdbWrxIL8phEagG0xuD0qsAzx7\nbmFBz2FdG4Ih29AoNugKbgFWbkDFXb5HpLWGy3N53QUnDPcmuK2vaC40y2fgSIp4\ndwzp1QBJ6b88QMordl9Z7q/3eQfJz4YUR1Ff0JGJgm5pX4K4zaWktxihUFvuULrH\ncijMBrexy5b+kDWBiTXMZ/xO0BNwfhDX7hvXfRf/YQKBgQDj+SB7F3mT0vBFUMAX\nQbkYKX2j0RU38nbQjKy7p2nyoo8bynN/JSLEN6+bHUc/w2O8wvfwqKlrqmdTp2cK\nzEMQF8zxo/4lGtaTM/mZXpuzQ5a06dFXOFlsDHDi0HUomWSLrIlpzi9eJUE0cVbp\ntXBbI9FXuN2G7POXhVFTLznKewKBgQDYnyhFtxobHpzQPAPW48hzdo62DVub2cVJ\nZqCADTy5zWuP6H4M4ZtuLeH5nHcUklU1ktAEQkmlaoJFoNnFgCwjgAUL0USnPzsA\nVVg1ic4wjtyKF7l0yzZ1qNwanabjzFe6mZh7DmvyXLMRR5BnnormFDnKVEe8K8Tv\nwc6ybQTANwKBgQDZXfjyZPevUzl1XgFKK4ho2WbKg9lPdwiC4R7x7ja++vCo9ugZ\nGzeDD/WIWOpOiebXXK878BVaaygKQ1ukmA55kbf/zvXMO0LNKImdCvA6hP91kFvi\nZgAdXd9k5I5RH7EW3HRJRic0BSGe91J/lFiFDinVEpdmxxecKanZPvZDcQKBgGra\n17rL/4yxJRlzAVHXOuH58ZvMKwxN+AulHDcOUI42zoBGZydjgpBvAbFnhTYYmP+8\nU/BF4p4+U9SU69eIyj0YwWR67iqx/iDD+KcwOw2o6xETcuAx/cM8cJaQfeqQuhXJ\n/nV1P7pmD87ORmVWx52HDJrJyDkCPjsmlxdcv6MxAoGAWQnr0iV7m0cEVy0GiM+k\nmnq6eXWITWC66zGUjC0JNa2eOY44Zv21HcqnrFU1w1AYpUZ6o03oHhOuwHiYsCXe\noQxg8WaFq9y8RbCZo5LqJi1a2eUvwI2xx7qnGiz5zttP2bi9yWsdbrD+e5Xvya9l\npUiDpZOiH2ZiN1vAJPvdBtc=\n-----END PRIVATE KEY-----\n",
            "client_email": SERVICE_ACCOUNT_EMAIL,
            "client_id": "100017389731197187312",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/google-calendar%40integration-project-420318.iam.gserviceaccount.com"
        },
        scopes=SCOPES
    )

    service = build('calendar', 'v3', credentials=creds)

    # Check if the calendar already exists
    calendar_list = service.calendarList().list().execute()
    for calendar_entry in calendar_list['items']:
        if calendar_entry['summary'] == 'Integration Project 2':
            print('Calendar already exists:', calendar_entry['id'])
            return calendar_entry['id'], f"https://calendar.google.com/calendar/embed?src={calendar_entry['id']}"  # Return the existing calendar ID and embed link

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

        # Add event to the newly created or existing calendar
        service.events().insert(calendarId=created_calendar['id'], body=event_body).execute()
        print('Event created for calendar:', created_calendar['id'])

    return created_calendar['id'], f"https://calendar.google.com/calendar/embed?src={created_calendar['id']}"  # Return the ID of the newly created calendar and its embed link

if __name__ == '__main__':
    calendar_id, embed_link = create_calendar()
    print('Calendar ID:', calendar_id)
    print('Embed Link:', embed_link)
