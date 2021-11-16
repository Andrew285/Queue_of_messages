import psycopg2  # postgreSQL package
from data import DataAPI
from managment import ManagmentAPI
from flask import Flask, redirect, url_for, request

app = Flask(__name__)


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
            self.W_API = DataAPI(min_node, self.M_API.m_address_id)  # data node with minimal amount of messages
            self.R_API = DataAPI(max_node, self.M_API.m_address_id)  # data node with maximum amount of messages
        self.counter = (self.counter + 1) % 100

    # add message to the database
    def push_message(self):
        self._update_stats()

        @app.route('/success/<name>')
        def success(name):
            return 'Message: %s added' % name

        @app.route('/login', methods=['POST', 'GET'])
        def login():
            if request.method == 'POST':
                text = request.form['nm']
                self.W_API.write_message(text)
                return redirect(url_for('success', name=text))
            else:
                text = request.args.get('nm')
                return redirect(url_for('success', name=text))

    # get message from the database
    def get_message(self):
        self._update_stats()

        @app.route('/get_mssg')
        def extra():
            return self.R_API.read_message()



client = Client('127.0.0.1:8011')

ip_address = "127.0.0.1"
port = 8011
text = "text_"


# client.get_message()
# client.push_message()
app.run()


