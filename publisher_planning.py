import xml.etree.ElementTree as ET
import mysql.connector
import pika
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Function to fetch user data from MySQL database based on user_id
def fetch_user_data(user_id):
    # Establish database connection
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_DATABASE'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    cursor = conn.cursor()

    # Fetch user data based on user_id
    query = "SELECT UserId, CalendarLink FROM User WHERE UserId = %s"
    cursor.execute(query, (int(user_id),))
    user_data = cursor.fetchone()

    # Close cursor and connection
    cursor.close()
    conn.close()

    return user_data

# Function to fetch event data from MySQL database based on event_id
def fetch_event_data(event_id):
    # Establish database connection
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_DATABASE'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

    cursor = conn.cursor()

    # Fetch event data based on event_id
    query = """
        SELECT EventId, EventDate, StartTime, EndTime, Location, SpeakerUserId, SpeakerCompanyId,
               MaxRegistrations, AvailableSeats, Description  
        FROM Events
        WHERE EventId = %s
    """                                                         #TODO: database columns need to be changed
    cursor.execute(query, (int(event_id),))
    event_data = cursor.fetchone()

    # Close cursor and connection
    cursor.close()
    conn.close()

    return event_data

# Function to publish XML object to RabbitMQ
def publish_xml_message(exchange_name, routing_key, xml_str):
    # Publish the XML object to RabbitMQ
    credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USER'), os.getenv('RABBITMQ_PASSWORD'))
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST'), credentials=credentials))
    channel = connection.channel()

    # Declare the exchange
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    # Publish the message
    channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=xml_str)

    # Close the connection
    connection.close()
        
    print(f"XML message published to RabbitMQ with routing key '{routing_key}'")


# Function to publish XML user-object to RabbitMQ
def publish_user_xml(user_id):
    # Fetch user data from MySQL database
    user_data = fetch_user_data(user_id)

    if user_data:
        user_id, calendar_link = user_data

        # Construct XML document based on schema
        user_elem = ET.Element('user')

         # Define all elements from the schema with empty values
        elements = [
            'routing_key', 'user_id', 'first_name', 'last_name', 'email',
            'telephone', 'birthday', 'company_email', 'company_id', 'source',
            'user_role', 'invoice', 'calendar_link'
        ]

        for elem_name in elements:
            ET.SubElement(user_elem, elem_name)

        # Add address element
        address_elem = ET.SubElement(user_elem, 'address')
        address_sub_elements = ['country', 'state', 'city', 'zip', 'street', 'house_number']
        for sub_elem_name in address_sub_elements:
            ET.SubElement(address_elem, sub_elem_name)

        # Set values for specific elements
        user_elem.find('user_id').text = str(user_id)
        user_elem.find('calendar_link').text = calendar_link or ''  # Use calendar_link or empty string

        # Create XML string
        xml_str = ET.tostring(user_elem, encoding='utf-8', method='xml')
        xml_str = xml_str.decode('utf-8')  # Convert bytes to string

        publish_xml_message('amq.topic', 'user.planning', xml_str)
        
        print(f"XML message published to RabbitMQ for user_id: {user_id}")
    else:
        print(f"User with user_id '{user_id}' not found in the database.")

# Function to publish event XML object to RabbitMQ
def publish_event_xml(event_id):
    # Fetch event data from MySQL database
    event_data = fetch_event_data(event_id)

    if event_data:
        (event_id, event_date, start_time, end_time, location, speaker_user_id, speaker_company_id,
         max_registrations, available_seats, description) = event_data

        # Construct XML document for event
        event_elem = ET.Element('event')

        # Define elements with empty values
        elements = [
            'routing_key', 'id', 'date', 'start_time', 'end_time', 'location', 'description',
            'max_registrations', 'available_seats'
        ]

        for elem_name in elements:
            ET.SubElement(event_elem, elem_name)

        # Add speaker element
        speaker_elem = ET.SubElement(event_elem, 'speaker')
        ET.SubElement(speaker_elem, 'user_id').text = str(speaker_user_id)
        ET.SubElement(speaker_elem, 'company_id').text = str(speaker_company_id)

        # Set values for specific elements
        event_elem.find('routing_key').text = 'frontend.crm'
        event_elem.find('id').text = str(event_id)
        event_elem.find('date').text = str(event_date)
        event_elem.find('start_time').text = str(start_time)
        event_elem.find('end_time').text = str(end_time)
        event_elem.find('location').text = str(location)
        event_elem.find('max_registrations').text = str(max_registrations)
        event_elem.find('available_seats').text = str(available_seats)
        event_elem.find('description').text = str(description)

        # Create XML string
        xml_str = ET.tostring(event_elem, encoding='utf-8', method='xml')
        xml_str = xml_str.decode('utf-8')  # Convert bytes to string

        # Publish the event XML object to RabbitMQ
        publish_xml_message('amq.topic', 'event.planning', xml_str)

    else:
        print(f"Event with event_id '{event_id}' not found in the database.")


# Example usage
if __name__ == '__main__':
    user_id_to_publish = '1'  # Provide the user_id for which you want to publish the XML
    publish_user_xml(user_id_to_publish)
