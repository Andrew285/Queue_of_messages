import psycopg2  # postgreSQL package
from data import DataAPI
from managment import ManagmentAPI


class Client:
    def __init__(self, node_address):
        self.node_address = node_address  # ip address/ For example {"127.0.0.1:8011"}
        self.stats = None  # statistics
        self.M_API = ManagmentAPI(node_address)  # management node
        self.W_API = None  # data node with minimal amount of messages
        self.R_API = None  # data node with maximum amount of messages
        self.counter = 0

    # check statistics of data nodes every 100 requests
    def _update_stats(self):
        if self.counter == 0:
            self.stats = self.M_API.get_stats()
            min_node = min(self.stats.keys(), key=(lambda x: self.stats[x]))
            max_node = max(self.stats.keys(), key=(lambda x: self.stats[x]))
            self.W_API = DataAPI(min_node)  # data node with minimal amount of messages
            self.R_API = DataAPI(max_node)  # data node with maximum amount of messages
        self.counter = (self.counter + 1) % 10

    # add message to the database
    def push_message(self, msg):
        self._update_stats()
        self.W_API.write_message(msg)

    # get message from the database
    def get_message(self):
        self._update_stats()
        return self.R_API.read_message()

    #add new node to database
    def push_data_noda(self, node_data_address, fk_id):
        self.M_API.add_node(node_data_address, fk_id)


client = Client('127.0.0.1:8011')

ip_address = "127.0.0.1"
port = 8011
text = "text_"
#
# client.push_data_noda("127.0.0.1:7011",1)
# client.push_data_noda("127.0.0.1:7012",1)
# client.push_data_noda("127.0.0.1:7013",1)

tmp_count = 0
for i in range(30):
    # client.push_message(f"number_{tmp_count}")
    print(client.get_message())
    tmp_count += 1


