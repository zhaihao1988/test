import sys
from sqlalchemy import create_engine

def get_db_engine(env='test'):
    """
    Creates a SQLAlchemy engine for the specified environment.
    
    Args:
        env (str): The environment to connect to, 'test' or 'uat'.
                   Defaults to 'test'.

    Returns:
        A SQLAlchemy engine object or None if creation fails.
    """
    
    conn_details = {
        "test": {
            "host": "10.128.21.148",
            "port": "5431",
            "dbname": "cas25_test",
            "user": "readonly_cas25_test",
            "password": "readonly_cas25_test"
        },
        "uat": {
            "host": "10.128.21.148", # Assuming same host, update if different
            "port": "5431", # Assuming same port, update if different
            "dbname": "cas25_uat",
            "user": "readonly_cas25_uat",
            "password": "readonly_cas25_uat"
        }
    }

    if env not in conn_details:
        print(f"Error: Environment '{env}' not recognized. Use 'test' or 'uat'.", file=sys.stderr)
        return None

    config = conn_details[env]
    
    try:
        url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
        print(f"Creating SQLAlchemy engine for {env} database '{config['dbname']}'...")
        engine = create_engine(url)
        # Test the connection
        with engine.connect() as connection:
            print(f"Successfully connected to the {env} database.")
        return engine
    except Exception as e:
        print(f"Error creating engine for the {env} database: {e}", file=sys.stderr)
        return None

if __name__ == '__main__':
    # Example of how to use this connector
    print("Testing database engines...")
    
    # Test 'test' environment
    test_engine = get_db_engine('test')
    if test_engine:
        print("SQLAlchemy engine for 'test' created successfully.")
        test_engine.dispose()
    else:
        print("Failed to create engine for the 'test' database.")

    print("-" * 20)

    # Test 'uat' environment
    uat_engine = get_db_engine('uat')
    if uat_engine:
        print("SQLAlchemy engine for 'uat' created successfully.")
        uat_engine.dispose()
    else:
        print("Failed to create engine for the 'uat' database.")
