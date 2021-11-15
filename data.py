import psycopg2
from SQL import cursor
import queue as q


class DataAPI:
    def __init__(self, node_address, fk_id):
        self.node_address = node_address  # ip address/ For example {"127.0.0.1:8011"}
        self.fk_data_id = fk_id  # reference data id for database (DO NOT DELETE!)

    def __repr__(self):
        return f"{self.node_address}, fk_id={self.fk_data_id}"

    def read_message(self):
        # return {}
        pass

    def write_message(self, mssg):
        # check if element is already in database
        cursor.execute(
            "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s",
            (self.node_address, self.fk_data_id)
        )
        fk_id = cursor.fetchone()
        if fk_id is None:
            cursor.execute(
                "INSERT INTO messages (message, fk_d_address) VALUES (%s, %s)", (mssg, fk_id)
            )
