import json
import psycopg2
from SQL import cursor
import requests



class DataAPI:
    def __init__(self, node_address, fk_id):
        self.node_address = node_address  # ip address/ For example {"127.0.0.1:8011"}
        self.fk_data_id = fk_id  # reference data id for database (DO NOT DELETE!)

    def __repr__(self):
        return f"{self.node_address}, fk_id={self.fk_data_id}"

    def read_message(self):
        msg = requests.get(f'http://{self.node_address}/read_message')
        return msg.text

    def write_message(self, msg):
        msg=msg+'|'+str(self.fk_data_id)
        requests.post(f'http://{self.node_address}/write_message', data=msg)