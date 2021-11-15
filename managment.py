import psycopg2
from SQL import cursor


class ManagmentAPI:
    def __init__(self, node_address):
        self.node_address = node_address  # ip address/ For example {"127.0.0.1:8011"}

        # check if element is already in database
        cursor.execute(
            "SELECT m_address_id FROM management_nodes WHERE m_address_ip = %s", (node_address,)
        )
        data_id = cursor.fetchone()
        if data_id is None:
            cursor.execute(
                "INSERT INTO management_nodes (m_address_ip) VALUES (%s)", (node_address,)
            )
        else:
            print(f"There is such management node with this ip_address: {node_address}")

        cursor.execute(
            "SELECT m_address_id FROM management_nodes WHERE m_address_ip = %s", (node_address,)
        )
        self.m_address_id = cursor.fetchone()[0]  # because it's tuple | it`s reference in the table

    # get statistics of data nodes (how many messages are in the data node (queue))
    def get_stats(self):
        d = {}  # dictionary for comfortability
        cursor.execute(
            "SELECT d_address_ip FROM data_nodes"
        )

        # writting ip addresses to the list (for comfortability)
        ip_list = []
        while True:
            row = cursor.fetchone()
            if row:
                ip_list.append(row[0])
            else:
                break

        # count messages from each data node
        count_list = []
        i = 1
        while i <= len(ip_list):
            cursor.execute(
                "SELECT COUNT(message) FROM messages WHERE fk_d_address = %s", (i,)
            )
            count_list.append(cursor.fetchone()[0])
            i += 1

        # writting ip addresses and results of counting to the dictionary
        for j in range(len(ip_list)):
            d.update({f"{ip_list[j]}": count_list[j]})

        # return dictionary
        return d

    # add new data node to the database
    def add_node(self, node_data_address, fk_id):

        # check if element is already in database
        cursor.execute(
            "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s",
            (node_data_address, fk_id)
        )
        id = cursor.fetchone()
        if id is None:
            cursor.execute(
                "INSERT INTO data_nodes (d_address_ip, fk_m_address) VALUES (%s, %s)", (node_data_address, fk_id)
            )
        else:
            print(f"Node with ip: {node_data_address} is already in database")

    # removes data node from database
    def remove_node(self, node_data_address):
        cursor.execute(
            "DROP FROM data_nodes WHERE d_address_ip = %s", (node_data_address,)
        )
