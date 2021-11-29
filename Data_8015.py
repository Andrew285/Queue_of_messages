from SQL import cursor
from flask import Flask, request

app = Flask(__name__)

id_counter = 2

@app.route("/read_message", methods=["GET"])
def read_message():
    global id_counter
    cursor.execute(
        # "SELECT message FROM messages WHERE message_id = %s AND fk_d_address = %s", (id_counter, 1)
        "SELECT message FROM messages WHERE fk_d_address = %s LIMIT 1", (3, )
    )
    text = cursor.fetchone()[0]
    cursor.execute(
        "DELETE FROM messages WHERE message = %s AND fk_d_address = %s", (text, 3)
    )
    id_counter += 1
    return text

@app.route("/write_message", methods=["POST"])
def write_message():
    msg = str(request.data)
    text, id = msg.split('|')
    id = id[:-1]
    id = int(id)
    # print(f"id: {id}")
    # check if element is already in database
    # cursor.execute("SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s",('127.0.0.1:8012', id))
    # fk_id = cursor.fetchone()[0]
    # print(fk_id)
    # if fk_id is None:
    cursor.execute(
        "INSERT INTO messages (message, fk_d_address) VALUES (%s, %s)", (text, 3)
    )
    return text


app.run(host='127.0.0.1', debug=True, port=8014)

