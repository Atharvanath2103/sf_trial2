import os, sys, logging
import json
import snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle = 'qmark'

def connect_snow():
    """Establishes a connection to Snowflake using a private key file."""
    
    private_key_path = os.getenv("PRIVATE_KEY_PATH", "rsa_key.p8")  # Default: rsa_key.p8 in same dir
    
    # Read the private key from file
    try:
        with open(private_key_path, "rb") as key_file:
            private_key_data = key_file.read()
        
        p_key = serialization.load_pem_private_key(
            private_key_data,
            password=None  # No password since key is not encrypted
        )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    
    except Exception as e:
        raise ValueError(f"Error loading private key from {private_key_path}: {e}")

    # Ensure required environment variables are set
    required_envs = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"]
    missing_envs = [env for env in required_envs if not os.getenv(env)]
    if missing_envs:
        raise ValueError(f"Missing environment variables: {', '.join(missing_envs)}")

    # Establish Snowflake connection
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-insert'},
    )

def save_to_snowflake(snow, message):
    """ Inserts a record into Snowflake from a JSON message. """
    try:
        record = json.loads(message.strip())  # Strip to remove unwanted newlines
        logging.debug('Inserting record to DB')

        row = (
            record['txid'], record['rfid'], record["resort"], record["purchase_time"],
            record["expiration_time"], record['days'], record['name'],
            json.dumps(record['address']), record['phone'], record['email'],
            json.dumps(record['emergency_contact'])
        )

        query = """
        INSERT INTO LIFT_TICKETS_PY_INSERT 
        ("TXID", "RFID", "RESORT", "PURCHASE_TIME", "EXPIRATION_TIME", "DAYS", "NAME", "ADDRESS", "PHONE", "EMAIL", "EMERGENCY_CONTACT") 
        SELECT ?,?,?,?,?,?,?,PARSE_JSON(?),?,?,PARSE_JSON(?)
        """

        with snow.cursor() as cur:
            cur.execute(query, row)

        logging.debug(f"Inserted ticket {record}")

    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON: {e}")
    except Exception as e:
        logging.error(f"Database insert error: {e}")

if __name__ == "__main__":
    try:
        snow = connect_snow()
        for message in sys.stdin:
            if message.strip():  # Ignore empty lines
                save_to_snowflake(snow, message)
        snow.close()
        logging.info("Ingest complete")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
