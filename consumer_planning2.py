import pika
from lxml import etree

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
    'event': """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="event">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="routing_key" type="xs:string"/>
                        <xs:element name="id" type="xs:string"/>
                        <xs:element name="date" type="xs:date"/>
                        <xs:element name="start_time" type="xs:string"/>
                        <xs:element name="end_time" type="xs:string"/>
                        <xs:element name="location" type="xs:string"/>
                        <xs:element name="speaker">
                            <xs:complexType>
                                <xs:sequence>
                                    <xs:element name="user_id" type="xs:string"/>
                                    <xs:element name="company_id" type="xs:string"/>
                                </xs:sequence>
                            </xs:complexType>
                        </xs:element>
                        <xs:element name="max_registrations" type="xs:integer"/>
                        <xs:element name="available_seats" type="xs:integer"/>
                        <xs:element name="description" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    """
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
            return True, None  # Valid XML
        else:
            return False, f"No schema available for the received XML root element '{root.tag}'"
    except etree.XMLSchemaError as e:
        return False, str(e)  # Invalid XML

# Callback function for consuming messages
def callback(ch, method, properties, body):
    try:
        # Decode message body (assuming it's in UTF-8)
        xml_content = body.decode('utf-8')
        
        # Validate the XML based on its root element
        is_valid, error_message = validate_xml(xml_content)
        
        if is_valid:
            print(f"Received valid XML:")
            print(xml_content)
        else:
            print(f"Received invalid XML: {error_message}")
    
    except Exception as e:
        print(f"Error processing message: {str(e)}")

# Connect to RabbitMQ server
credentials = pika.PlainCredentials('user', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.2.160.51', credentials=credentials))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='planning', durable=True)

# Set up consumer to receive messages from the queue
channel.basic_consume(queue='planning', on_message_callback=callback, auto_ack=True)

# Start consuming messages
print('Waiting for messages...')
channel.start_consuming()