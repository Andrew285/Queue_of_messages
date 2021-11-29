from SQL import cursor
from flask import Flask, request

app = Flask(__name__)


@app.route("/read_message", methods=["GET"])
def read_message():
    # get address_id from the data_node
    cursor.execute(
        "SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s", (request.host,)
    )
    d_id = cursor.fetchone()[0]

    # get message_id from the message to delete of data_node
    cursor.execute(
        "SELECT message_id FROM messages WHERE fk_d_address = %s LIMIT 1", (d_id,)
    )
    m_id = cursor.fetchone()[0]

    # get text from message to delete to output on console
    cursor.execute(
        "SELECT message FROM messages WHERE fk_d_address = %s LIMIT 1", (d_id,)
    )
    text = cursor.fetchone()[0]

    # delete the message from the queue (data_node)
    cursor.execute(
        "DELETE FROM messages WHERE message_id = %s", (m_id,)
    )
    return text


@app.route("/write_message", methods=["POST"])
def write_message():
    msg = str(request.data)
    text, id_ = msg.split('|')
    id_ = id_[:-1]
    id_ = int(id_)  # get id for specific message in database

    # insert new message in database
    cursor.execute(
        "INSERT INTO messages (message, fk_d_address) VALUES (%s, %s)", (text, id_)
    )
    return text


app.run(host='127.0.0.1', debug=True, port=7011)
