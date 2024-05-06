# DEZE CONSUMER NIET MEER GEBRUIKEN!!!



import pika
import pymysql
from xml.etree import ElementTree as ET

# RabbitMQ connection parameters
credentials = pika.PlainCredentials('user', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
channel = connection.channel()

# Queue declaration
channel.queue_declare(queue='planning', durable=True)

# Database connection parameters
db_host = '172.17.0.6'
db_user = 'root'
db_password = 'password'
db_name = 'test1'

# Function to insert XML data into corresponding database table (users or companies)
def insert_xml_data_into_db(xml_data):
    try:
        # Establish database connection
        db_connection = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
        cursor = db_connection.cursor()

        # Parse XML data
        root = ET.fromstring(xml_data)

        # Determine message type (user or company)
        if root.tag == 'user':
            table_name = 'users'
        elif root.tag == 'company':
            table_name = 'companies'
        elif root.tag == 'event':
            table_name = 'events'
        else:
            print(f"Unknown message type: {root.tag}")
            return

        # Extract common fields
        routing_key = root.find('routing_key').text.strip()
        address = root.find('address')
        country = address.find('country').text.strip()
        state = address.find('state').text.strip()
        city = address.find('city').text.strip()
        zip_code = address.find('zip').text.strip()
        street = address.find('street').text.strip()
        house_number = address.find('house_number').text.strip()

        # Extract specific fields based on message type
        if table_name == 'users':
            user_id = root.find('user_id').text.strip()
            first_name = root.find('first_name').text.strip()
            last_name = root.find('last_name').text.strip()
            email = root.find('email').text.strip()
            telephone = root.find('telephone').text.strip()
            birthday = root.find('birthday').text.strip()
            company_email = root.find('company_email').text.strip() if root.find('company_email') is not None else None

            sql = f"""
                INSERT INTO user (user_id, first_name, last_name, email, telephone, birthday, country, state, city, zip_code, street, house_number, company_email, routing_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (user_id, first_name, last_name, email, telephone, birthday, country, state, city, zip_code, street, house_number, company_email, routing_key))
        elif table_name == 'companies':
            company_id = root.find('id').text.strip()
            name = root.find('name').text.strip()
            email = root.find('email').text.strip() if root.find('email') is not None else None
            telephone = root.find('telephone').text.strip() if root.find('telephone') is not None else None
            logo = root.find('logo').text.strip() if root.find('logo') is not None else None
            company_type = root.find('type').text.strip()
            invoice = root.find('invoice').text.strip()

            sql = f"""
                INSERT INTO company (company_id, name, email, telephone, logo, country, state, city, zip_code, street, house_number, company_type, invoice, routing_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (company_id, name, email, telephone, logo, country, state, city, zip_code, street, house_number, company_type, invoice, routing_key))

        db_connection.commit()
        print(f"{root.tag.capitalize()} data inserted into database successfully.")

    except Exception as e:
        print(f"Error inserting {root.tag.capitalize()} data into database:", e)
    finally:
        cursor.close()
        db_connection.close()

# Callback function to process incoming messages
def callback(ch, method, properties, body):
    xml_message = body.decode('utf-8')  # Decode message from bytes to string
    print(' [x] Received XML message:', xml_message)

    # Call function to insert XML data into corresponding database table
    insert_xml_data_into_db(xml_message)

channel.basic_consume('planning', callback, auto_ack=True)

print(' [*] Waiting for XML messages. To exit, press CTRL+C')

# Start consuming messages indefinitely
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('Interrupted. Closing connection.')
    connection.close()

