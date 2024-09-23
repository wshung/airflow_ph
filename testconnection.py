import psycopg2

try:
    conn = psycopg2.connect(
        dbname='p_lake',
        user='ylg_paul',
        password='8gXPeJj/sUdgi6gx/1Dqsw==',
        host='10.140.0.2',
        port='65430'
    )
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
