import os
import pika
from lxml import etree
from dotenv import load_dotenv
import mysql.connector
import calendar_events

# Load environment variables from .env file
load_dotenv()

# Embedded XSD schemas
XSD_SCHEMAS = {
    'user': """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="user">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="routing_key">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:minLength value="1"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="crud_operation">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="create"/>
                                    <xs:enumeration value="update"/>
                                    <xs:enumeration value="delete"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="id">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:minLength value="1"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="first_name" type="xs:string" nillable="true"/>
                        <xs:element name="last_name" type="xs:string" nillable="true"/>
                        <xs:element name="email" type="xs:string" nillable="true"/>
                        <xs:element name="telephone" type="xs:string" nillable="true"/>
                        <xs:element name="birthday">
                            <xs:simpleType>
                                <xs:union>
                                    <xs:simpleType>
                                        <xs:restriction base='xs:string'>
                                            <xs:length value="0"/>
                                        </xs:restriction>
                                    </xs:simpleType>
                                    <xs:simpleType>
                                        <xs:restriction base='xs:date' />
                                    </xs:simpleType>
                                </xs:union>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="address">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="country" type="xs:string" nillable="true"/>
                                    <xs:element name="state" type="xs:string" nillable="true"/>
                                    <xs:element name="city" type="xs:string" nillable="true"/>
                                    <xs:element name="zip">
                                        <xs:simpleType>
                                            <xs:union>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:string'>
                                                        <xs:length value="0"/>
                                                    </xs:restriction>
                                                </xs:simpleType>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:integer' />
                                                </xs:simpleType>
                                            </xs:union>
                                        </xs:simpleType>
                                    </xs:element>
                                    <xs:element name="street" type="xs:string" nillable="true"/>
                                    <xs:element name="house_number">
                                        <xs:simpleType>
                                            <xs:union>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:string'>
                                                        <xs:length value="0"/>
                                                    </xs:restriction>
                                                </xs:simpleType>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:integer' />
                                                </xs:simpleType>
                                            </xs:union>
                                        </xs:simpleType>
                                    </xs:element>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                        <xs:element name="company_email" type="xs:string" nillable="true"/>
                        <xs:element name="company_id" type="xs:string" nillable="true"/>
                        <xs:element name="source" type="xs:string"  nillable="true"/>
                        <xs:element name="user_role">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="speaker"/>
                                    <xs:enumeration value="individual"/>
                                    <xs:enumeration value="employee"/>
                                    <xs:enumeration value=""/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="invoice" type="xs:string" nillable="true"/>
                        <xs:element name="calendar_link" type="xs:string" nillable="true"/>
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
                        <xs:element name="routing_key">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:minLength value="1"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="crud_operation">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="create"/>
                                    <xs:enumeration value="update"/>
                                    <xs:enumeration value="delete"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="id">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:minLength value="1"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="name" type="xs:string" nillable="true"/>
                        <xs:element name="email" type="xs:string" nillable="true"/>
                        <xs:element name="telephone" type="xs:string" nillable="true"/>
                        <xs:element name="logo" type="xs:string" nillable="true"/>
                        <xs:element name="address">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="country" type="xs:string" nillable="true"/>
                                    <xs:element name="state" type="xs:string" nillable="true"/>
                                    <xs:element name="city" type="xs:string" nillable="true"/>
                                    <xs:element name="zip">
                                        <xs:simpleType>
                                            <xs:union>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:string'>
                                                        <xs:length value="0"/>
                                                    </xs:restriction>
                                                </xs:simpleType>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:integer' />
                                                </xs:simpleType>
                                            </xs:union>
                                        </xs:simpleType>
                                    </xs:element>
                                    <xs:element name="street" type="xs:string" nillable="true"/>
                                    <xs:element name="house_number">
                                        <xs:simpleType>
                                            <xs:union>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:string'>
                                                        <xs:length value="0"/>
                                                    </xs:restriction>
                                                </xs:simpleType>
                                                <xs:simpleType>
                                                    <xs:restriction base='xs:integer' />
                                                </xs:simpleType>
                                            </xs:union>
                                        </xs:simpleType>
                                    </xs:element>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                        <xs:element name="type">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="customer"/>
                                    <xs:enumeration value="sponsor"/>
                                    <xs:enumeration value="speaker"/>
                                    <xs:enumeration value=""/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="invoice" type="xs:string" nillable="true"/>
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
                        <xs:element name="routing_key">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:minLength value="1"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="crud_operation">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:enumeration value="create"/>
                                    <xs:enumeration value="update"/>
                                    <xs:enumeration value="delete"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="id">
                            <xs:simpleType>
                                <xs:restriction base="xs:string">
                                    <xs:minLength value="1"/>
                                </xs:restriction>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="user_id" type="xs:string" nillable="true"/>
                        <xs:element name="event_id" type="xs:string" nillable="true"/>
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
        id_elem = root_element.find('id')
        first_name_elem = root_element.find('first_name')
        last_name_elem = root_element.find('last_name')
        email_elem = root_element.find('email')
        company_id_elem = root_element.find('company_id')
        
        if id_elem is not None and first_name_elem is not None \
           and last_name_elem is not None and email_elem is not None:
            
            # Extract text values from XML elements
            user_id = id_elem.text
            first_name = first_name_elem.text
            last_name = last_name_elem.text
            email = email_elem.text
            
            # Check if company_id is available
            if company_id_elem is not None:
                company_id = company_id_elem.text
            else:
                company_id = None  # Set company_id to None if not provided
            
            conn = get_database_connection()
            cursor = conn.cursor()

            # Prepare SQL query and values
            sql = "INSERT INTO User (UserId, First_name, Last_name, Email, CompanyId) VALUES (%s, %s, %s, %s, %s)"
            values = (user_id, first_name, last_name, email, company_id)
            
            # Execute SQL query
            cursor.execute(sql, values)
            conn.commit()
            
            print("User data saved to the database successfully.")
            
            cursor.close()
            conn.close()
        
        else:
            print("One or more required elements (id, first_name, last_name, email) are missing in the XML.")
    
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

#Function to extract attendance data and funtioncall to system
def send_attendance_to_system(root_element):
    try:
        user_id = root_element.find('user_id').text
        event_id = root_element.find('event_id').text
        
        # Call function to add event to calendar using extracted user_id and event_id
        calendar_events.add_event_to_calendar(user_id, event_id)
        
        print("Event added to calendar successfully.")
    except Exception as e:
        print(f"Error adding event to calendar: {str(e)}")

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
                send_attendance_to_system(root_element)
            else:
                print(f"No handler defined for XML type: {xml_type}")
        else:
            print(f"Received invalid XML: {root_element}")
    
    except Exception as e:
        print(f"Error processing message: {str(e)}")

# Connect to RabbitMQ server
credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USER'), os.getenv('RABBITMQ_PASSWORD'))
connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.getenv('RABBITMQ_HOST'), credentials=credentials))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='planning', durable=True)

# Set up consumer to receive messages from the queue
channel.basic_consume(queue='planning', on_message_callback=callback, auto_ack=True)

# Start consuming messages
print('Waiting for messages...')
channel.start_consuming()