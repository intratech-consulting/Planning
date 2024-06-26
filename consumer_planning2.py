from datetime import datetime
import os
import pika
from lxml import etree
from dotenv import load_dotenv
import mysql.connector
import calendar_events
import logging
import sys
import requests
import json
import publisher_planning

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
                        <xs:element name="sponsor" type="xs:boolean" nillable="true"/>
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
    'event':"""
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="event">
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
                        <xs:element name="title" type="xs:string" nillable="true"/>
                        <xs:element name="date">
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
                        <xs:element name="start_time">
                            <xs:simpleType>
                                <xs:union>
                                    <xs:simpleType>
                                        <xs:restriction base='xs:string'>
                                            <xs:length value="0"/>
                                        </xs:restriction>
                                    </xs:simpleType>
                                    <xs:simpleType>
                                        <xs:restriction base='xs:time' />
                                    </xs:simpleType>
                                </xs:union>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="end_time">
                            <xs:simpleType>
                                <xs:union>
                                    <xs:simpleType>
                                        <xs:restriction base='xs:string'>
                                            <xs:length value="0"/>
                                        </xs:restriction>
                                    </xs:simpleType>
                                    <xs:simpleType>
                                        <xs:restriction base='xs:time' />
                                    </xs:simpleType>
                                </xs:union>
                            </xs:simpleType>
                        </xs:element>
                        <xs:element name="location" type="xs:string" nillable="true"/>
                        <xs:element name="speaker">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="user_id" type="xs:string" nillable="true"/>
                                    <xs:element name="company_id" type="xs:string" nillable="true"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                        <xs:element name="max_registrations">
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
                        <xs:element name="available_seats">
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
                        <xs:element name="description" type="xs:string" nillable="true"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    """,
}

headers = {
        "Content-Type": "application/json"  # Set content type to JSON
    }

def add_service_id(master_uuid, service, service_id):
    url = f"http://{os.getenv('RABBITMQ_HOST')}:6000/addServiceId"
    payload = {
        "MasterUuid": master_uuid,
        "Service": service,
        "ServiceId": service_id
    }

   

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        
        if response.status_code in (200, 201):
            return response.json()
        else:
            logger.error(f"Unexpected response: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during request: {e}")
        return None

def delete_service_id(master_uuid, service):
    url = f"http://{os.getenv('RABBITMQ_HOST')}:6000/updateServiceId"
    payload = {
        "MASTERUUID": master_uuid,
        "NewServiceId": None,
        "Service": service
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    else:
        return None
    

def get_service_id(master_uuid, service_name):
    url = f"http://{os.getenv('RABBITMQ_HOST')}:6000/getServiceId"
    payload = {
        "MASTERUUID": master_uuid,
        "Service": service_name
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()[service_name]
    else:
        return None
    
# Function to validate XML against embedded XSD schema
def validate_xml(xml_str):
    try:
        logger.debug(xml_str)
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
        logger.debug(e)
        return False, str(e)  # Invalid XML

# Function to establish a database connection
def get_database_connection():
    logger.debug(os.getenv('DB_HOST'))
    logger.debug(os.getenv('DB_USER'))
    logger.debug(os.getenv('DB_PASSWORD'))
    logger.debug(os.getenv('DB_DATABASE'))
    
    try: 
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 3306),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        logger.debug(connection)
        logger.info(connection)  # This line is not recommended; use it for debugging only
        publisher_planning.sendLogsToMonitoring("connection_to_database_consumer", connection, False)

        return connection  # Return the connection object if successful
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        # If an exception occurs during connection, handle it here
        # For example, log the error and return None or raise the exception

        # Handle the error appropriately based on your application's needs
        # Example:
        # raise  # Rethrow the exception or return None, depending on your error handling strategy
        return None  # Return None if connection fails (handle this appropriately in the caller)

# Function to save user data to the database
def save_user_to_database(root_element):
    try:
        crud_operation = root_element.find('crud_operation').text
        id_elem = root_element.find('id')
        
        if crud_operation not in ['create', 'update', 'delete']:
            logger.error(f"Invalid CRUD operation: {crud_operation}")
            return
        
        if crud_operation == 'delete':
            routing_key_elem = root_element.find('routing_key')
            if routing_key_elem is None or routing_key_elem.text != 'user.facturatie':
                logger.error("Invalid routing key for delete operation. Expected 'user.facturatie'.")
                return

            if id_elem is None:
                logger.error("User ID not provided for delete operation.")
                return
            
            user_id = id_elem.text
            conn = get_database_connection()
            
            if conn is not None:
                cursor = conn.cursor()
                sql = "DELETE FROM User WHERE UserId = %s"
                cursor.execute(sql, (user_id,))
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f"User data with ID '{user_id}' deleted successfully.")
                logs = f"User data with ID '{user_id}' deleted successfully."
                publisher_planning.sendLogsToMonitoring("User_deleted", logs, False)
                delete_service_id(user_id,'planning')
            else:
                logger.error("Database connection failed. Unable to delete user data.")
        
        else:  # For create and update operations
            if id_elem is None:
                logger.error("User ID not provided for create/update operation.")
                return
                
            first_name_elem = root_element.find('first_name')
            last_name_elem = root_element.find('last_name')
            email_elem = root_element.find('email')
            company_id_elem = root_element.find('company_id')

            if first_name_elem is None or last_name_elem is None or email_elem is None:
                logger.error("One or more required elements (first_name, last_name, email) are missing in the XML.")
                return

            user_id = id_elem.text
            first_name = first_name_elem.text
            last_name = last_name_elem.text
            email = email_elem.text

            if company_id_elem is not None:
                company_id = company_id_elem.text
            else:
                company_id = None

            conn = get_database_connection()

            if conn is not None:
                cursor = conn.cursor()

                if crud_operation == 'create':
                    sql = "INSERT INTO User (UserId, First_name, Last_name, Email, CompanyId) VALUES (%s, %s, %s, %s, %s)"
                    values = (user_id, first_name, last_name, email, company_id)
                    cursor.execute(sql, values)
                    conn.commit()
                    logger.info("User data saved to the database successfully.")
                    log_create = "User data saved to the database successfully."
                    publisher_planning.sendLogsToMonitoring("User_created", log_create, False)
                elif crud_operation == 'update':
                    select_user = "SELECT First_name, Last_name, Email, CompanyId FROM User WHERE UserId = %s"
                    cursor.execute(select_user, (user_id,))
                    current_data = cursor.fetchone()
                    if current_data is None:
                        logger.error(f"User with ID '{user_id}' not found.")
                    else:
                        current_first_name, current_last_name, current_email, current_company_id = current_data

                        # Use current data if no new data is provided
                        first_name = first_name if first_name is not None else current_first_name
                        last_name = last_name if last_name is not None else current_last_name
                        email = email if email is not None else current_email
                        company_id = company_id if company_id is not None else current_company_id

                sql = "UPDATE User SET First_name = %s, Last_name = %s, Email = %s, CompanyId = %s WHERE UserId = %s"
                values = (first_name, last_name, email, company_id, user_id)
                cursor.execute(sql, values)
                conn.commit()
                logger.info(f"User data with ID '{user_id}' updated successfully.")
                log_update = f"User data with ID '{user_id}' updated successfully."
                publisher_planning.sendLogsToMonitoring("User_updated", log_update, False)


                cursor.close()
                conn.close()

                # Add service ID and create calendar event for new users
                if crud_operation == 'create':
                    logger.info("TestTest")
                    add_service_id(user_id, 'planning', user_id)
                    calendar_events.create_calendar(user_id)

            else:
                logger.error("Database connection failed. Unable to save user data.")

    except Exception as e:
        logger.error(f"Error saving user data to database: {str(e)}")

# Function to save company data to the database
def save_company_to_database(root_element):
    try:
        crud_operation = root_element.find('crud_operation').text
        id_elem = root_element.find('id')

        if crud_operation not in ['create', 'update', 'delete']:
            logger.error(f"Invalid CRUD operation: {crud_operation}")
            return
        
        if id_elem is None:
            logger.error("Company ID not provided.")
            return
        
        company_id = id_elem.text

        conn = get_database_connection()
        if conn is None:
            logger.error("Database connection failed. Unable to perform the operation.")
            return

        cursor = conn.cursor()

        if crud_operation == 'delete':
            sql = "DELETE FROM Company WHERE CompanyId = %s"
            cursor.execute(sql, (company_id,))
            conn.commit()
            delete_service_id(company_id,'planning')
            logger.info(f"Company data with ID '{company_id}' deleted successfully.")
            log_delete = f"Company data with ID '{company_id}' deleted successfully."
            publisher_planning.sendLogsToMonitoring("Company_Deleted", log_delete, False)
        
        else:  # For create and update operations
            name_elem = root_element.find('name')
            email_elem = root_element.find('email')
            

            if name_elem is None or email_elem is None:
                logger.error("One or more required elements (name, email) are missing in the XML.")
                return

            name = name_elem.text
            email = email_elem.text

            if crud_operation == 'create':
                sql = "INSERT INTO Company (CompanyId, Name, Email) VALUES (%s, %s, %s)"
                values = (company_id, name, email)
                cursor.execute(sql, values)
                conn.commit()
                add_service_id(company_id, 'planning', company_id)
                logger.info("Company data saved to the database successfully.")
                log_create = "Company data saved to the database successfully."
                publisher_planning.sendLogsToMonitoring("Company_created", log_create, False)
            
            elif crud_operation == 'update':
                select_company = "SELECT Name, Email FROM Company WHERE CompanyId = %s"
                cursor.execute(select_company, (company_id,))
                current_data = cursor.fetchone()
                
                if current_data is None:
                    logger.error(f"Company with ID '{company_id}' not found.")
                else:
                    current_name, current_email = current_data

                    # Use current data if no new data is provided
                    name = name if name is not None else current_name
                    email = email if email is not None else current_email

                    sql = "UPDATE Company SET Name = %s, Email = %s WHERE CompanyId = %s"
                    values = (name, email, company_id)
                    cursor.execute(sql, values)
                    conn.commit()
                    logger.info(f"Company data with ID '{company_id}' updated successfully.")
                    log_update = f"Company data with ID '{company_id}' updated successfully."
                    publisher_planning.sendLogsToMonitoring("Company_updated", log_update, False)

        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error saving company data to database: {str(e)}")


def handle_event(root_element):
    try:
        # Extract the CRUD operation
        crud_operation = root_element.find('crud_operation').text if root_element.find('crud_operation') is not None else None

        if crud_operation not in ['create', 'update', 'delete']:
            logger.error(f"Invalid CRUD operation: {crud_operation}")
            return
        
        # Extract the event ID
        id_elem = root_element.find('id')
        event_id = id_elem.text if id_elem is not None else None

        if event_id is None:
            logger.error("Event ID not provided.")
            return

        # Get the database connection
        conn = get_database_connection()
        if conn is None:
            logger.error("Database connection failed. Unable to perform the operation.")
            return
        
        cursor = conn.cursor()

        if crud_operation == 'create':
            # Extract event details
            summary = root_element.find('title').text if root_element.find('title') is not None else None
            date = root_element.find('date').text if root_element.find('date') is not None else None
            start_time = root_element.find('start_time').text if root_element.find('start_time') is not None else None
            end_time = root_element.find('end_time').text if root_element.find('end_time') is not None else None
            location = root_element.find('location').text if root_element.find('location') is not None else None
            description = root_element.find('description').text if root_element.find('description') is not None else None
            max_registrations = root_element.find('max_registrations').text if root_element.find('max_registrations') is not None else None
            available_seats = root_element.find('available_seats').text if root_element.find('available_seats') is not None else None

            # Ensure numeric values are properly converted
            max_registrations = int(max_registrations) if max_registrations is not None else None
            available_seats = int(available_seats) if available_seats is not None else None

            # Compose start_datetime and end_datetime
            start_datetime = None
            end_datetime = None
            if date and start_time:
                start_datetime = datetime.strptime(f"{date} {start_time}", "%Y-%m-%d %H:%M:%S")
            if date and end_time:
                end_datetime = datetime.strptime(f"{date} {end_time}", "%Y-%m-%d %H:%M:%S")
            # Insert event data into the database
            sql = """
                INSERT INTO Events (summary, start_datetime, end_datetime, location, description, max_registrations, available_seats)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (summary, start_datetime, end_datetime, location, description, max_registrations, available_seats)
            cursor.execute(sql, values)
            conn.commit()

            cursor.execute("SELECT LAST_INSERT_ID()")
            service_event_id = cursor.fetchone()[0]
            logger.info("Event data successfully saved to database")

            add_service_id(event_id, 'planning', service_event_id)

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error saving event data to database: {str(e)}")

#Function to extract attendance data and funtioncall to system
def send_attendance_to_system(root_element):
    try:
        master_user_id = root_element.find('user_id').text
        master_event_id = root_element.find('event_id').text

        user_id = get_service_id(master_user_id, 'planning')
        event_id = get_service_id(master_event_id, 'planning')

        crud_operation = root_element.find('crud_operation').text

        if crud_operation == 'create':
            # Call function to add event to calendar using extracted user_id and event_id
            calendar_events.add_event_to_calendar(user_id, event_id)
            logger.info("Event added to calendar successfully.")
            log = "Event added to calendar successfully."
            publisher_planning.sendLogsToMonitoring("Send_attendance", log, False)

        elif crud_operation == 'delete':
            # Call function to delete event from calendar using extracted user_id and event_id
            calendar_events.delete_event_by_id(user_id, event_id)
            logger.info("Event deleted from calendar successfully.")
            log = "Event deleted from calendar successfully."
            publisher_planning.sendLogsToMonitoring("Delete_attendance", log, False)

        else:
            log = f"Invalid CRUD operation: {crud_operation}"
            logger.error(log)
            publisher_planning.sendLogsToMonitoring("Invalid_operation", log, True)

    except Exception as e:
        error_message = f"Error processing attendance: {str(e)}"
        logger.error(error_message)
        publisher_planning.sendLogsToMonitoring("Error_processing_attendance", error_message, True)


# Callback function for consuming messages
def callback(ch, method, properties, body):
    try:
        xml_content = body.decode('utf-8')
        is_valid, root_element = validate_xml(xml_content)
        logger.debug(xml_content)
        
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
            elif xml_type == 'event':
                handle_event(root_element)
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