import mysql.connector
from mysql.connector import Error



def create_connection():
    """Make database connection"""
    try:
        # Connect to MySQL server without specifying a database
        connection = mysql.connector.connect(
            host='mysql',
            port=3306,
            user='root',
            password='mypassword'
        )
        cursor = connection.cursor()

        # Check if the 'masterUuid' database exists and create it if it doesn't
        cursor.execute("CREATE DATABASE IF NOT EXISTS planning")
        connection.commit()

        # Close the connection and cursor
        cursor.close()
        connection.close()

        # Connect to the 'masterUuid' database
        connection = mysql.connector.connect(
            host='mysql',
            port=3306,
            user='root',
            password='mypassword',
            database='planning'
        )
        print("Connected successfully to the database.")
        return connection
    except Error as e:
        error_message = f"DB-connection Error: '{e}'"
        print(error_message)
        return error_message  # Return the error message

def create_tables():
    """Create Company, User, and Events tables"""
    connection = create_connection()
    if isinstance(connection, str):  # Check if connection is an error message
        return {"error": connection}  # Return error as JSON response

    cursor = connection.cursor()

    # Define SQL queries to create tables
    queries = [
        """
        CREATE TABLE IF NOT EXISTS Company (
            CompanyId INT PRIMARY KEY,
            Name VARCHAR(100),
            Email VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS User (
            UserId INT AUTO_INCREMENT PRIMARY KEY,
            First_name VARCHAR(50),
            Last_name VARCHAR(50),
            Email VARCHAR(100),
            CompanyId VARCHAR(255),
            CalendarId VARCHAR(255),
            CalendarLink VARCHAR(255),
            
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Events (
            Id INT AUTO_INCREMENT PRIMARY KEY,
            Summary VARCHAR(255),
            Start_datetime DATETIME,
            End_datetime DATETIME,
            Location VARCHAR(100),
            Description TEXT,
            Max_Registrations INT,
            Available_Seats INT
        )
        """
    ]

    try:
        for query in queries:
            cursor.execute(query)
        connection.commit()
        print("Tables successfully created.")
        return {"success": True, "message": "Tables successfully created."}
    except Error as e:
        error_message = f"Failed to create tables: {e}"
        print(error_message)
        return {"error": error_message}  # Return error as JSON response
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    create_tables()
