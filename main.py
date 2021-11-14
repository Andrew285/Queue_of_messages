import psycopg2  #postgreSQL package

#connection to exist database Heroku service DO NOT DELETE
connection = psycopg2.connect(
    host = "ec2-44-198-236-169.compute-1.amazonaws.com",
    user = "cuoytssinpguhx",
    password = "7e126c52b4ac8af7ff202059092a10e277da2f4db9ca10029dfbd3639a6d651d",
    database = "ddka0da87h182a",
    port = 5432
)
connection.autocommit = True

#creating table of management nodes
cursor = connection.cursor()
cursor.execute(
        """CREATE TABLE IF NOT EXISTS management_nodes(
            m_address_id serial PRIMARY KEY,
            m_address_ip text NOT NULL
        );"""
    )

#creating table of data nodes
cursor.execute(
        """CREATE TABLE IF NOT EXISTS data_nodes(
            d_address_id serial PRIMARY KEY,
            d_address_ip text NOT NULL,
            fk_m_address integer NOT NULL
        );"""
    )

#creating table of messages
cursor.execute(
        """CREATE TABLE IF NOT EXISTS messages(
            message_id serial PRIMARY KEY,
            message text NOT NULL,
            fk_d_address integer NOT NULL
        );"""
    )

#------------------------------MANAGEMENT_API-------------------------------------------------------

class ManagmentAPI:
    def __init__(self, node_address):
        self.node_address = node_address  #ip address/ For example {"127.0.0.1:8011"}

        #check if element is already in database
        cursor.execute(
            "SELECT m_address_id FROM management_nodes WHERE m_address_ip = %s", (node_address,)
        )
        data_id = cursor.fetchone()
        if data_id is None:
            cursor.execute(
                "INSERT INTO management_nodes (m_address_ip) VALUES (%s)", (node_address, )
            )
        else:
            print(f"There is such management node with this ip_address: {node_address}")

        cursor.execute(
            "SELECT m_address_id FROM management_nodes WHERE m_address_ip = %s", (node_address, )
        )
        self.m_address_id = cursor.fetchone()[0]  #because it's tuple | it`s reference in the table

    #get statistics of data nodes (how many messages are in the data node (queue))
    def get_stats(self):
        d = {}  #dictionary for comfortability
        cursor.execute(
            "SELECT d_address_ip FROM data_nodes"
        )

        #writting ip addresses to the list (for comfortability)
        ip_list = []
        while True:
            row = cursor.fetchone()
            if row:
                ip_list.append(row[0])
            else:
                break

        #count messages from each data node
        count_list = []
        i = 1
        while i <= len(ip_list):
            cursor.execute(
                "SELECT COUNT(message) FROM messages WHERE fk_d_address = %s", (i,)
            )
            count_list.append(cursor.fetchone()[0])
            i += 1

        #writting ip addresses and results of counting to the dictionary
        for j in range(len(ip_list)):
            d.update({f"{ip_list[j]}": count_list[j]})

        #return dictionary
        return d

    #add new data node to the database
    def add_node(self, node_data_address, fk_id):

        #check if element is already in database
        cursor.execute(
            "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s", (node_data_address, fk_id)
        )
        id = cursor.fetchone()
        if id is None:
            cursor.execute(
                "INSERT INTO data_nodes (d_address_ip, fk_m_address) VALUES (%s, %s)", (node_data_address, fk_id)
            )
        else:
            print(f"Node with ip: {node_data_address} is already in database")

    #removes data node from database
    def remove_node(self, node_data_address):
        cursor.execute(
            "DROP FROM data_nodes WHERE d_address_ip = %s", (node_data_address,)
        )

#----------------------------------------DATA_API-----------------------------------------------------------------------

class DataAPI:
    def __init__(self, node_address, fk_id):
        self.node_address = node_address  #ip address/ For example {"127.0.0.1:8011"}
        self.fk_data_id = fk_id  #reference data id for database (DO NOT DELETE!)
    def __repr__(self):
        return f"{self.node_address}, fk_id={self.fk_data_id}"

    def read_message(self):
        # return {}
        pass

    def write_message(self, mssg):
        #check if element is already in database
        cursor.execute(
            "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s", (self.node_address, self.fk_data_id)
        )
        fk_id = cursor.fetchone()
        if fk_id is None:
            cursor.execute(
                "INSERT INTO messages (message, fk_d_address) VALUES (%s, %s)", (mssg, fk_id)
            )

#---------------------------------------------------CLIENT--------------------------------------------------------------------------

class Client:
    def __init__(self, mnode_address):
        self.mnode_address = mnode_address  #ip address/ For example {"127.0.0.1:8011"}
        self.stats = None  #statistics
        self.M_API = ManagmentAPI(mnode_address)  #management node
        self.W_API = None  #data node with minimal amount of messages
        self.R_API = None  #data node with maximum amount of messages
        self.counter = 0

    #check statistics of data nodes every 100 requests
    def _update_stats(self):
        if self.counter == 0:
            self.stats = self.M_API.get_stats()
            min_node = min(self.stats.keys(), key=(lambda x: self.stats[x]))
            max_node = max(self.stats.keys(), key=(lambda x: self.stats[x]))
            self.W_API = DataAPI(min_node, self.M_API.m_address_id)  #data node with minimal amount of messages
            self.R_API = DataAPI(max_node, self.M_API.m_address_id)  #data node with maximum amount of messages
        self.counter = (self.counter + 1) % 100

    #add message to the database
    def push_message(self, mssg):
        self._update_stats()
        self.W_API.write_message(mssg)

    #get message from the database
    def get_message(self):
        self._update_stats()
        return self.R_API.read_message()
