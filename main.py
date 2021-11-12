import psycopg2
import queue as q

#connection to exist database
connection = psycopg2.connect(
    host = "ec2-44-198-236-169.compute-1.amazonaws.com",
    user = "cuoytssinpguhx",
    password = "7e126c52b4ac8af7ff202059092a10e277da2f4db9ca10029dfbd3639a6d651d",
    database = "ddka0da87h182a",
    port = 5432
)
connection.autocommit = True

# connection = psycopg2.connect("dbname=postgres user=postgres host=localhost password=user port=5432")
# connection.autocommit = True

cursor = connection.cursor()
cursor.execute(
        """CREATE TABLE IF NOT EXISTS management_nodes(
            m_address_id serial PRIMARY KEY,
            m_address_ip text NOT NULL
        );"""
    )


cursor.execute(
        """CREATE TABLE IF NOT EXISTS data_nodes(
            d_address_id serial PRIMARY KEY,
            d_address_ip text NOT NULL,
            fk_m_address integer NOT NULL
        );"""
    )

cursor.execute(
        """CREATE TABLE IF NOT EXISTS messages(
            message_id serial PRIMARY KEY,
            message text NOT NULL,
            fk_d_address integer NOT NULL
        );"""
    )


class ManagmentAPI:
    def __init__(self, node_address):
        self.node_address = node_address
        self.queue = q.Queue()

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
        self.m_address_id = cursor.fetchone()[0]  #because it's tuple

    def get_stats(self):
        d = {}
        cursor.execute(
            "SELECT d_address_ip FROM data_nodes"
        )
        ip_list = []
        while True:
            row = cursor.fetchone()
            if row:
                ip_list.append(row[0])
            else:
                break
        count_list = []
        i = 1
        while i <= len(ip_list):
            cursor.execute(
                "SELECT COUNT(message) FROM messages WHERE fk_d_address = %s", (i,)
            )
            count_list.append(cursor.fetchone()[0])
            i += 1

        for j in range(len(ip_list)):
            d.update({f"{ip_list[j]}": count_list[j]})

        return d

    def add_node(self, node_data_address, fk_id):
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

    def remove_node(self, node_data_address):
        cursor.execute(
            "DROP FROM data_nodes WHERE d_address_ip = %s", (node_data_address,)
        )

class DataAPI:
    def __init__(self, node_address, fk_id):
        self.node_address = node_address
        self.fk_data_id = fk_id
    def __repr__(self):
        return f"{self.node_address}, fk_id={self.fk_data_id}"

        # cursor.execute(
        #     "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s", (node_address, fk_id)
        # )
        # data_id = cursor.fetchone()
        # if data_id is None:
        #     cursor.execute(
        #         "INSERT INTO data_nodes (d_address_ip, fk_m_address) VALUES (%s, %s)", (node_address, fk_id)
        #     )
        # else:
        #     print(f"There is such data node with this ip_address: {node_address}")

    def read_message(self):
        # return {}
        pass
    def write_message(self, mssg):
        cursor.execute(
            "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s", (self.node_address, self.fk_data_id)
        )
        fk_id = cursor.fetchone()
        if fk_id is None:
            cursor.execute(
                "INSERT INTO messages (message, fk_d_address) VALUES (%s, %s)", (mssg, fk_id)
            )

class Client:
    def __init__(self, mnode_address):
        self.mnode_address = mnode_address
        self.stats = None
        self.M_API = ManagmentAPI(mnode_address)
        self.W_API = None
        self.R_API = None
        self.counter = 0

    def _update_stats(self):
        if self.counter == 0:
            self.stats = self.M_API.get_stats()
            min_node = min(self.stats.keys(), key=(lambda x: self.stats[x]))
            max_node = max(self.stats.keys(), key=(lambda x: self.stats[x]))
            self.W_API = DataAPI(min_node, self.M_API.m_address_id)
            self.R_API = DataAPI(max_node, self.M_API.m_address_id)
        self.counter = (self.counter + 1) % 100

    def push_message(self, mssg):
        self._update_stats()
        self.W_API.write_message(mssg)

    def get_message(self):
        self._update_stats()
        return self.R_API.read_message()


# cl1 = Client('127.0.0.1:8011')

# ip_address = "127.0.0.1"
# port = 8011
# text = "text_"
#
# n = 5
# for i in range(n):
#     text += str(i)
#     port += 1
#     cl1.M_API.add_node(f"{ip_address}:{port}", cl1.M_API.m_address_id)

# for j in range(125):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 1)
#     )
#
# for j in range(156):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 2)
#     )
#
# for j in range(140):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 3)
#     )
#
# for j in range(190):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 4)
#     )
#
# for j in range(110):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 5)
#     )

# d = cl1.M_API.get_stats()
# print(d)
#
# cl1._update_stats()
# print(cl1.R_API)
# print(cl1.W_API)
#
#
# cl1.get_message()
# cl1.push_message("test")