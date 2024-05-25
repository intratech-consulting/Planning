import json
import re
import time
import xml.etree.ElementTree as ET
import mysql.connector
import pika
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime

import requests

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

headers = {
    "Content-Type": "application/json"
}

def create_master_uuid(service_id, service_name):
    url = f"http://{os.getenv('RABBITMQ_HOST')}:6000/createMasterUuid"
    payload = {
        "ServiceId": service_id,
        "Service": service_name
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200 and response.json().get("success") or response.status_code == 201 and response.json().get("success"):
        return response.json().get("MasterUuid")
    else:
        return None

# Function to fetch user data from MySQL database based on user_id
def fetch_user_data(user_id):
    # Establish database connection
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 3306),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE')
    )

    cursor = conn.cursor()

    # Fetch user data based on user_id
    query = "SELECT UserId, CalendarLink FROM User WHERE UserId = %s"
    cursor.execute(query, (user_id,))
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
        port=os.getenv('DB_PORT', 3306),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE')
    )

    cursor = conn.cursor()

    # Fetch event data based on event_id
    query = """
        SELECT Id, Summary, Start_datetime, End_datetime, Location, Description, Max_Registrations, Available_Seats
        FROM Events
        WHERE id = %s
    """
    cursor.execute(query, (event_id,))
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
        
    logger.info(f"XML message published to RabbitMQ with routing key '{routing_key}'")


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
            'routing_key', 'crud_operation', 'id', 'first_name', 'last_name', 'email',
            'telephone', 'birthday', 'address', 'company_email', 'company_id', 'source',
            'user_role', 'invoice', 'calendar_link'
        ]

        for elem_name in elements:
            ET.SubElement(user_elem, elem_name).text = ''

        # Add address element
        address_elem = user_elem.find('address')
        address_sub_elements = ['country', 'state', 'city', 'zip', 'street', 'house_number']
        for sub_elem_name in address_sub_elements:
            ET.SubElement(address_elem, sub_elem_name).text = ''
            
        # Set values for specific elements
        user_elem.find('id').text = str(user_id)
        user_elem.find('calendar_link').text = calendar_link or ''  # Use calendar_link or empty string
        user_elem.find('crud_operation').text = 'update'  # Set the 'crud_operation' value to 'create'
        user_elem.find('routing_key').text = 'user.planning'  # Set the 'routing_key' value to 'user.planning'

        # Create XML string
        xml_str = ET.tostring(user_elem, encoding='utf-8', method='xml')
        xml_str = xml_str.decode('utf-8')  # Convert bytes to string

        # Ensure all empty elements are represented with explicit opening and closing tags
        xml_str = re.sub(r'<(\w+)\s*/>', r'<\1></\1>', xml_str)

        publish_xml_message('amq.topic', 'user.planning', xml_str)
        
        print(f"XML message published to RabbitMQ for user_id: {user_id}")
    else:
        print(f"User with user_id '{user_id}' not found in the database.")

# Function to publish XML event object to RabbitMQ
def publish_event_xml(results):
    logger.info("Entered Publisher")

    time.sleep(10) # Sleep for 5 seconds to make sure the event is inserted into the database
    # Fetch event data from MySQL database
    # event_data = fetch_event_data(event_id)

    if results:
        (id, title, start_datetime, end_datetime, location, description, max_registrations, available_seats) = results

        event_id = create_master_uuid(id, 'planning')

        # Extract date and time components
        event_date = start_datetime.date()
        start_time = start_datetime.time()
        end_time = end_datetime.time()

        # Construct XML document for event
        event_elem = ET.Element('event')

        # Define elements with extracted values
        elements = [
            ('routing_key', 'event.planning'),
            ('crud_operation', 'create'),
            ('id', str(event_id)),
            ('title', str(title)),
            ('date', str(event_date)),
            ('start_time', str(start_time)),
            ('end_time', str(end_time)),
            ('location', location),
            ('description', description),
        ]

        for elem_name, elem_value in elements:
            ET.SubElement(event_elem, elem_name).text = elem_value

        # Add speaker element with sub-elements between location and max_registrations
        speaker_elem = ET.SubElement(event_elem, 'speaker')
        ET.SubElement(speaker_elem, 'user_id').text = ''
        ET.SubElement(speaker_elem, 'company_id').text = ''

        # Add max_registrations and available_seats elements
        ET.SubElement(event_elem, 'max_registrations').text = str(max_registrations)
        ET.SubElement(event_elem, 'available_seats').text = str(available_seats)

        # Create XML string
        xml_str = ET.tostring(event_elem, encoding='utf-8', method='xml')
        xml_str = xml_str.decode('utf-8')  # Convert bytes to string

        # Ensure all empty elements are represented with explicit opening and closing tags
        xml_str = re.sub(r'<(\w+)\s*/>', r'<\1></\1>', xml_str)

        # Publish the event XML object to RabbitMQ
        publish_xml_message('amq.topic', 'event.planning', xml_str)

    else:
        print(f"Event with event_id '{id}' not found in the database.")


def sendLogsToMonitoring(functionName, logMessage, isError):
    # Construct XML document for LogEntry
    log_elem = ET.Element('LogEntry')

    # Define elements with provided values
    elements = [
        ('SystemName', 'ExampleSystem'),
        ('FunctionName', str(functionName)),
        ('Logs', str(logMessage)),
        ('Error', 'true' if isError else 'false'),
        ('Timestamp', datetime.now().isoformat()),  # Timestamp in ISO 8601 format
    ]

    for elem_name, elem_value in elements:
        ET.SubElement(log_elem, elem_name).text = elem_value

    # Create XML string
    xml_str = ET.tostring(log_elem, encoding='utf-8', method='xml')
    xml_str = xml_str.decode('utf-8')  # Convert bytes to string

    # Publish Log XML object to RabbitMQ
    publish_xml_message('amq.topic', 'logs', xml_str)


# Example usage
#if __name__ == '__main__':
 #   event_id_to_publish = '1'  # Provide the user_id for which you want to publish the XML
  #  publish_event_xml(event_id_to_publish)
