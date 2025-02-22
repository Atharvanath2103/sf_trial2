import snowflake.connector

try:
    conn = snowflake.connector.connect(
        account="qtdimqf-xe86232",
        user="your_snowflake_username",
        password="your_password",
        warehouse="INGEST",
        database="INGEST",
        schema="INGEST",
    )
    print("Connected successfully!")
except Exception as e:
    print(f"Error: {e}")