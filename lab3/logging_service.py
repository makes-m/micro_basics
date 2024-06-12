import hazelcast
from flask import Flask, request
import argparse
import subprocess, signal

app = Flask(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

p = subprocess.Popen(['./start.sh'])

hz = hazelcast.HazelcastClient()

messages = hz.get_map("messages").blocking()

@app.route("/logging", methods=["POST", "GET"])
def log_request():
    if request.method == "POST":
        _id = request.form['id']
        _msg = request.form['msg']
        messages.put(_id, _msg)
        print("Received message:", _msg) 
        return "Success"
    elif request.method == "GET":
        keys = messages.key_set()
        for key in keys:
            value = messages.get(key)
            array[key] = value
        return array
    else:
        abort(400)

if __name__ == '__main__':
    try:
        app.run(port=args.port)
    except KeyboardInterrupt:
        p.send_signal(signal.SIGINT)



from flask import Flask
import hazelcast
import argparse

app = Flask(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

# Connect to the Hazelcast cluster
hz = hazelcast.HazelcastClient()

# Create a distributed queue for message delivery
queue_name = "message-queue"
queue = hz.get_queue(queue_name).blocking()

# Keep reading messages from the queue and storing them in the Hazelcast Map
messages = hz.get_map("messages").blocking()

@app.route('/messages', methods=['GET'])
def static_message():
    while True:
        try:
            msg = queue.take().result()
            _id = msg['id']
            _msg = msg['msg']
            messages.put(_id, _msg)
        except Exception as e:
            print("Error while reading message from queue:", e)
    keys = messages.key_set()
    array = []
    for key in keys:
        value = messages.get(key)
        array.append(value)
    return "\n".join(array)

if __name__ == '__main__':
    app.run(port=args.port)

"""
from flask import Flask
import hazelcast
import argparse

app = Flask(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

#hz = hazelcast.HazelcastClient()
#queue_name = "message-queue"
#queue = hz.get_queue(queue_name).blocking()

@app.route('/messages', methods=['GET'])
def get_message():
    messages = []
    while True:
        hz = hazelcast.HazelcastClient()
        queue_name = "message-queue"
        queue = hz.get_queue(queue_name).blocking()

        message = queue.poll()
        if not message:
            break
        messages.append(message)
        print(message)
    return {"messages": messages}
    #return "Not implemented yet"

if __name__ == '__main__':
    app.run(port=args.port)
 
"""
