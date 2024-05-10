import os
import pika
from lxml import etree
from dotenv import load_dotenv
import mysql.connector

# Load environment variables from .env file
load_dotenv()

# Embedded XSD schemas
XSD_SCHEMAS = {
    'user': """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="user">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="routing_key" type="xs:string"/>
                        <xs:element name="user_id" type="xs:string"/>
                        <xs:element name="first_name" type="xs:string"/>
                        <xs:element name="last_name" type="xs:string"/>
                        <xs:element name="email" type="xs:string"/>
                        <xs:element name="telephone" type="xs:string"/>
                        <xs:element name="birthday" type="xs:date"/>
                        <xs:element name="address">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="country" type="xs:string"/>
                                    <xs:element name="state" type="xs:string"/>
                                    <xs:element name="city" type="xs:string"/>
                                    <xs:element name="zip" type="xs:string"/>
                                    <xs:element name="street" type="xs:string"/>
                                    <xs:element name="house_number" type="xs:string"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                        <xs:element name="company_email" type="xs:string" minOccurs="0"/>
                        <xs:element name="company_id" type="xs:string" minOccurs="0"/>
                        <xs:element name="source" type="xs:string" minOccurs="0"/>
                        <xs:element name="user_role">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="Speaker"/>
                                    <xs:enumeration value="Individual"/>
                                    <xs:enumeration value="Employee"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="invoice">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="Yes"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    """,
    'company': """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="company">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="routing_key" type="xs:string"/>
                        <xs:element name="id" type="xs:string"/>
                        <xs:element name="name" type="xs:string"/>
                        <xs:element name="email" type="xs:string" minOccurs="0"/>
                        <xs:element name="telephone" type="xs:string" minOccurs="0"/>
                        <xs:element name="logo" type="xs:string" minOccurs="0"/>
                        <xs:element name="address">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="country" type="xs:string"/>
                                    <xs:element name="state" type="xs:string"/>
                                    <xs:element name="city" type="xs:string"/>
                                    <xs:element name="zip" type="xs:string"/>
                                    <xs:element name="street" type="xs:string"/>
                                    <xs:element name="house_number" type="xs:string"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                        <xs:element name="type">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="customer"/>
                                    <xs:enumeration value="sponsor"/>
                                    <xs:enumeration value="speaker"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="invoice">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="Yes"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    """,
    'attendance':"""
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="attendance">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="routing_key" type="xs:string"/>
                        <xs:element name="id" type="xs:string"/>
                        <xs:element name="user_id" type="xs:string"/>
                        <xs:element name="event_id" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    """,
}

# Function to validate XML against embedded XSD schema
def validate_xml(xml_str):
    try:
        root = etree.fromstring(xml_str)
        if root.tag in XSD_SCHEMAS:
            xsd_str = XSD_SCHEMAS[root.tag]
            xmlschema = etree.XMLSchema(etree.fromstring(xsd_str))
            xmlparser = etree.XMLParser(schema=xmlschema)
            etree.fromstring(xml_str, xmlparser)
            return True, root  # Valid XML and root element
        else:
            return False, f"No schema available for the received XML root element '{root.tag}'"
    except etree.XMLSchemaError as e:
        return False, str(e)  # Invalid XML

# Function to establish a database connection
def get_database_connection():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE')
    )

# Function to save user data to the database
def save_user_to_database(root_element):
    try:
        user_id = root_element.find('user_id').text
        first_name = root_element.find('first_name').text
        last_name = root_element.find('last_name').text
        email = root_element.find('email').text
        company_id = root_element.find('company_id').text
        
        conn = get_database_connection()
        cursor = conn.cursor()

        sql = "INSERT INTO User (UserId, First_name, Last_name, Email, CompanyId) VALUES (%s, %s, %s, %s, %s)"
        values = (user_id, first_name, last_name, email, company_id)
        cursor.execute(sql, values)

        conn.commit()
        cursor.close()
        conn.close()
        print("User data saved to the database successfully.")
    except Exception as e:
        print(f"Error saving user data to database: {str(e)}")

# Function to save company data to the database
def save_company_to_database(root_element):
    try:
        company_id = root_element.find('id').text
        name = root_element.find('name').text
        email = root_element.find('email').text
        
        conn = get_database_connection()
        cursor = conn.cursor()

        sql = "INSERT INTO Company (CompanyId, Name, Email) VALUES (%s, %s, %s)"
        values = (company_id, name, email)
        cursor.execute(sql, values)

        conn.commit()
        cursor.close()
        conn.close()
        print("Company data saved to the database successfully.")
    except Exception as e:
        print(f"Error saving company data to database: {str(e)}")

def save_attendance_to_database(root_element):
    try:
        user_id = root_element.find('user_id').text
        event_id = root_element.find('event_id').text
        
        conn = get_database_connection()
        cursor = conn.cursor()

        sql = "INSERT INTO Attendance (UserId, EventId) VALUES (%s, %s, %s)"
        values = (user_id, event_id)
        cursor.execute(sql, values)

        conn.commit()
        cursor.close()
        conn.close()
        print("Attendance data saved to the database successfully.")
    except Exception as e:
        print(f"Error saving attendance data to database: {str(e)}")


# Callback function for consuming messages
def callback(ch, method, properties, body):
    try:
        xml_content = body.decode('utf-8')
        is_valid, root_element = validate_xml(xml_content)
        
        if is_valid:
            xml_type = root_element.tag
            print(f"Received valid '{xml_type}' XML:")
            print(xml_content)
            
            if xml_type == 'user':
                save_user_to_database(root_element)
            elif xml_type == 'company':
                save_company_to_database(root_element)
            elif xml_type == 'attendance':
                save_attendance_to_database(root_element)
            else:
                print(f"No handler defined for XML type: {xml_type}")
        else:
            print(f"Received invalid XML: {root_element}")
    
    except Exception as e:
        print(f"Error processing message: {str(e)}")

# Connect to RabbitMQ server
credentials = pika.PlainCredentials('RABBITMQ_USER', 'RABBITMQ_PASSWORD')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='RABBITMQ_HOST', credentials=credentials))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='planning', durable=True)

# Set up consumer to receive messages from the queue
channel.basic_consume(queue='planning', on_message_callback=callback, auto_ack=True)

# Start consuming messages
print('Waiting for messages...')
channel.start_consuming()