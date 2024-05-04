import pika
import os
import xml.etree.ElementTree as ET  # Importing ElementTree to handle XML

# RabbitMQ connection parameters
credentials = pika.PlainCredentials('user', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
channel = connection.channel()



# Declare the 'user' queue
channel.queue_declare(queue='user', durable=True)


# Create the XML user-object
user_xml = """
<user>
    <user_id>3</user_id>
    <first_name>John</first_name>
    <last_name>Doe</last_name>
    <email>john.doe@mail.com</email>
    <telephone>+32467179912</telephone>
    <birthday>2024-04-14</birthday>
    <address>
        <country>Belgium</country>
        <state>Brussels</state>
        <city>Brussels</city>
        <zip>1000</zip>
        <street>Nijverheidskaai</street>
        <house_number>170</house_number>
    </address>
    <company_email>john.doe@company.com</company_email>
    <company_id></company_id>
    <source></source>
    <user_role></user_role>
    <invoice></invoice>
</user>
"""

# Publish the XML user-object as a message
channel.basic_publish(
    exchange='',
    routing_key='user',
    body=user_xml  # Set the message body as the serialized XML string
)

print('Message sent.')
channel.close()
connection.close()