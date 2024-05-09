import pika
from sqlalchemy import create_engine, MetaData, Table, select, Column
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# RabbitMQ connection parameters
credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USER'), os.getenv('RABBITMQ_PASSWORD'))
connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST'), credentials=credentials))
channel = connection.channel()

# Declare the 'user' queue
channel.queue_declare(queue='user', durable=True)

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# MySQL database connection string
DB_CONNECTION_STRING = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'

# Establish a database connection
engine = create_engine(DB_CONNECTION_STRING)

# Reflect the table from the database
metadata = MetaData()
table_name = 'User'
table = Table(table_name, metadata, autoload_with=engine)

# Specify the columns to extract
columns_to_extract = ['UserId', 'CalendarLink']

# Build a list of column objects for the select statement
columns = [table.c[column] for column in columns_to_extract]

# Construct the select statement with the list of columns
select_stmt = select(*columns)

# Construct the XML user-object with specific fields populated
with engine.connect() as conn:
    for row in conn.execute(select_stmt):
        user_xml = ET.Element('user')

        # Populate specific fields from the database
        for column in columns_to_extract:
            # Dynamically retrieve column value from the row tuple
            value = str(getattr(row, column)) if getattr(row, column) is not None else ''
            ET.SubElement(user_xml, column.lower()).text = value
        
        # Hardcode empty elements
        empty_elements = [
            'routing_key', 'first_name', 'last_name', 'email', 'telephone',
            'birthday', 'company_email', 'company_id', 'source', 'user_role',
            'invoice', 'calendar_id'
        ]
        for element in empty_elements:
            ET.SubElement(user_xml, element).text = ''

        # Serialize the XML object to a string
        user_xml_string = ET.tostring(user_xml, encoding='unicode')

        # Publish the XML user-object as a message
        channel.basic_publish(
            exchange='amq.topic',  # Use default exchange
            routing_key='user.frontend',
            body=user_xml_string  # Set the message body as the serialized XML string
        )
        print(f'Message sent for user_id: {row["UserId"]}')

# Close database connection
engine.dispose()
print('All messages sent.')

# Close RabbitMQ connection
channel.close()
connection.close()
