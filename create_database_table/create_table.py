from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("HOST")
port = int(os.getenv("PORT"))
db_name = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


try:

    engine = create_engine(connection_string)

    print("Connecting to database...")
    with engine.connect() as conn:
        conn.execute(text("SELECT version()"))
        pass
    print("Connection successful!")

    with open("create_table.sql", "r") as f:
        sql_statements = f.read()

    # with engine.begin() as conn:
    #     for statement in sql_statements.split(";"):
    #         if statement.strip():
    #             conn.execute(text(statement))

    with open("create_table.sql", "r") as f, engine.begin() as conn:
        sql_script = f.read()
        conn.execute(text(sql_script))
    print("Tables and functions created successfully!")

    print("Tables created successfully!")

except Exception as e:
    print("Error:")
    if "OperationalError" in str(e):  # Check for connection errors
        print("Failed to connect to database!")
    else:
        print(e)
