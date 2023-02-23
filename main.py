from paho.mqtt import client as mqtt_client
from time import strftime
import os
from dotenv import load_dotenv

def log_to_file(log):
    print(log)
    with open('log.txt', 'a') as log_file:
        log_file.write(f'[{strftime("%d/%m/%Y %H:%M:%S")}] {log}\n')

def connect_mqtt(broker, port, client_id, username, password) -> mqtt_client.Client:
    def on_connect_cb(client, userdata, flags, rc):
        if rc == 0:
            log_to_file('Connected to MQTT broker!')
        else:
            log_to_file(f'Failed to connect, return code {rc}')
    
    client = mqtt_client.Client(client_id, clean_session=False)
    client.username_pw_set(username, password)
    client.on_connect = on_connect_cb
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client.Client, topic):
    def on_msg_cb(client, userdata, msg):
        log_to_file(f'{msg.topic} - {msg.payload.decode()}')
    
    client.subscribe(topic, qos=1, )
    client.on_message = on_msg_cb

def credentials():
    broker = os.getenv('MQTT_SERVADDR')
    port = int(os.getenv('MQTT_SERVPORT'))
    client_id = 'py-mqtt-reader'
    username = os.getenv('MQTT_USER')
    passwd = os.getenv('MQTT_PWD')
    
    return broker, port, client_id, username, passwd

def main():
    try:
        topic = 'dragino_gateway'
        client = connect_mqtt(*credentials())
        subscribe(client, topic)
        client.loop_forever()
    except KeyboardInterrupt:
        log_to_file('Program exited! Reason: program was killed.')
    except Exception as e:
        log_to_file(f'EXCEPTION! Reason: {e}')

if __name__ == '__main__':
    load_dotenv()
    main()