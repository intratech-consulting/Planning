from lxml import etree
import pika, sys, os
import time
from datetime import datetime
import logging
from dotenv import load_dotenv

TEAM = 'planning'
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('heartbeat.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USER'), os.getenv('RABBITMQ_PASSWORD'))
    parameters = pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST'), credentials=credentials)
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            logger.info('Connected to RabbitMQ')
            return connection
        except pika.exceptions.AMQPConnectionError:
            logger.error('Failed to connect to RabbitMQ, retrying in 5 seconds...')
            time.sleep(5)

def main(timestamp, channel):
    global TEAM
    # Define your XML and XSD as strings
    heartbeat_xml = f'''
    <Heartbeat>
        <Timestamp>{timestamp.isoformat()}</Timestamp>
        <Status>Active</Status>
        <SystemName>{TEAM}</SystemName>
    </Heartbeat>
    '''
    heartbeat_xsd = f'''
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="Heartbeat">
            <xs:complexType>
                <xs:sequence>
                    <xs:element name="Timestamp" type="xs:dateTime" />
                    <xs:element name="Status" type="xs:string" />
                    <xs:element name="SystemName" type="xs:string" />
                </xs:sequence>
            </xs:complexType>
        </xs:element>
    </xs:schema>
    '''
    # Parse the documents
    xml_doc = etree.fromstring(heartbeat_xml.encode())
    xsd_doc = etree.fromstring(heartbeat_xsd.encode())
    # Create a schema object
    schema = etree.XMLSchema(xsd_doc)
    # Validate
    if schema.validate(xml_doc):
        logger.info('XML is valid')
    else:
        logger.error('XML is not valid')
        return
    
    # Publish to RabbitMQ
    try:
        channel.basic_publish(exchange='', routing_key='heartbeat_queue', body=heartbeat_xml)
        logger.info('Heartbeat message sent')
    except pika.exceptions.AMQPConnectionError:
        logger.error('Failed to send heartbeat message, connection error')

if __name__ == '__main__':
    try:
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue='heartbeat_queue', durable=True)
        
        while True:
            main(datetime.now(), channel)
            time.sleep(1)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except pika.exceptions.AMQPConnectionError:
        logger.error('Lost connection to RabbitMQ, attempting to reconnect...')
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue='heartbeat_queue', durable=True)
            