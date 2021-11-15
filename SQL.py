import psycopg2

# connection to exist database Heroku service DO NOT DELETE
connection = psycopg2.connect(
    host="ec2-52-204-72-14.compute-1.amazonaws.com",
    user="itmjvhkoihtrjq",
    password="86ea3005e1ca022fe1e173bb9d8b6f801bfbfa87019f8d5664b080509ba83787",
    database="d2107g3nvhtdlr",
    port=5432
)
connection.autocommit = True

# creating table of management nodes
cursor = connection.cursor()
cursor.execute(
    """CREATE TABLE IF NOT EXISTS management_nodes(
        m_address_id serial PRIMARY KEY,
        m_address_ip text NOT NULL
    );"""
)

# creating table of data nodes
cursor.execute(
    """CREATE TABLE IF NOT EXISTS data_nodes(
        d_address_id serial PRIMARY KEY,
        d_address_ip text NOT NULL,
        fk_m_address integer NOT NULL
    );"""
)

# creating table of messages
cursor.execute(
    """CREATE TABLE IF NOT EXISTS messages(
        message_id serial PRIMARY KEY,
        message text NOT NULL,
        fk_d_address integer NOT NULL
    );"""
)
