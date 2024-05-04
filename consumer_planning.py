import pika
import pymysql
from xml.etree import ElementTree as ET

# RabbitMQ connection parameters
credentials = pika.PlainCredentials('user', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
channel = connection.channel()

# Queue declaration
channel.queue_declare(queue='user', durable=True)

# Database connection parameters
db_host = '172.17.0.6' 
db_user = 'root'
db_password = 'password'
db_name = 'test1'

# Function to insert user data from XML into database
def insert_user_into_db(xml_data):
    try:
        # Establish database connection
        db_connection = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
        cursor = db_connection.cursor()

        # Parse XML data
        root = ET.fromstring(xml_data)

        # Extract user information from XML
        user_id = root.find('user_id').text.strip() if root.find('user_id') is not None and root.find('user_id').text is not None else None
        first_name = root.find('first_name').text.strip() if root.find('first_name') is not None and root.find('first_name').text is not None else None
        last_name = root.find('last_name').text.strip() if root.find('last_name') is not None and root.find('last_name').text is not None else None
        email = root.find('email').text.strip() if root.find('email') is not None and root.find('email').text is not None else None
        telephone = root.find('telephone').text.strip() if root.find('telephone') is not None and root.find('telephone').text is not None else None
        birthday = root.find('birthday').text.strip() if root.find('birthday') is not None and root.find('birthday').text is not None else None
        country = root.find('address/country').text.strip() if root.find('address/country') is not None and root.find('address/country').text is not None else None
        state = root.find('address/state').text.strip() if root.find('address/state') is not None and root.find('address/state').text is not None else None
        city = root.find('address/city').text.strip() if root.find('address/city') is not None and root.find('address/city').text is not None else None
        zip_code = root.find('address/zip').text.strip() if root.find('address/zip') is not None and root.find('address/zip').text is not None else None
        street = root.find('address/street').text.strip() if root.find('address/street') is not None and root.find('address/street').text is not None else None
        house_number = root.find('address/house_number').text.strip() if root.find('address/house_number') is not None and root.find('address/house_number').text is not None else None
        company_email = root.find('company_email').text.strip() if root.find('company_email') is not None and root.find('company_email').text is not None else None

        # Insert user data into database
        sql = """
            INSERT INTO users (user_id, first_name, last_name, email, telephone, birthday, country, state, city, zip_code, street, house_number, company_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (user_id, first_name, last_name, email, telephone, birthday, country, state, city, zip_code, street, house_number, company_email))

        db_connection.commit()
        print("User data inserted into database successfully.")

    except Exception as e:
        print("Error inserting user data into database:", e)
    finally:
        cursor.close()
        db_connection.close()


# Callback function to process incoming messages
def callback(ch, method, properties, body):
    xml_message = body.decode('utf-8')  # Decode message from bytes to string
    print(' [x] Received XML message:', xml_message)

    # Call function to insert user data into database
    insert_user_into_db(xml_message)

channel.basic_consume(
  'user',
  callback,
  auto_ack=True)

print(' [*] Waiting for XML messages. To exit, press CTRL+C')

# Start consuming messages indefinitely
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('Interrupted. Closing connection.')
    connection.close()


    #test commit
