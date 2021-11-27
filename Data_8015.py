from SQL import cursor
from flask import Flask, request

app = Flask(__name__)



@app.route("/read_message", methods=["GET"])
def read_message():
    return 'iiii'


@app.route("/write_message", methods=["POST"])
def write_message():
    msg = str(request.data)
    text, id = msg.split('|')
    id = id[:-1]
    id = int(id)
    # check if element is already in database
    cursor.execute("SELECT d_address_id FROM data_nodes WHERE d_address_ip = %s AND fk_m_address = %s",('127.0.0.1:8012', id))
    fk_id = cursor.fetchone()[0]
    print(fk_id)
    if fk_id is None:
        cursor.execute(
            "INSERT INTO messages (message, fk_d_address) VALUES (%s, %s)", (text, id)
        )
    return text


app.run(host='127.0.0.1', debug=True, port=8012)

