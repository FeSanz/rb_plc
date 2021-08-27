# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import argparse
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
from uuid import uuid4
import json
from pymodbus.client.sync import ModbusTcpClient as ModbusClient

# This sample uses the Message Broker for AWS IoT to send and receive messages
# through an MQTT connection. On startup, the device connects to the server,
# subscribes to a topic, and begins publishing messages to that topic.
# The device should receive those same messages back from the message broker,
# since it is subscribed to that same topic.

#variables temporales para validacion de cambios
temp_presion = 0
temp_temperatura = 0
temp_porcentaje = 0
temp_ciclos = 0
temp_alarma = 0

#conexión a modbus con ip-puerto
#client = ModbusClient('192.168.1.200', port=502)

parser = argparse.ArgumentParser(description="Send and receive messages through and MQTT connection.")
parser.add_argument('--endpoint', default="a19egjpzgi9ikd-ats.iot.us-west-1.amazonaws.com", help="Your AWS IoT custom endpoint, not including a port. " +
                                                      "Ex: \"abcd123456wxyz-ats.iot.us-east-1.amazonaws.com\"")
parser.add_argument('--port', type=int, help="Specify port. AWS IoT supports 443 and 8883.")
parser.add_argument('--cert', default = "af895ae3f3926b675823a573f233c8ec4722b7e508bc987f96e3234249f7880b-certificate.pem.crt", help="File path to your client certificate, in PEM format.")
parser.add_argument('--key', default="af895ae3f3926b675823a573f233c8ec4722b7e508bc987f96e3234249f7880b-private.pem.key", help="File path to your private key, in PEM format.")
parser.add_argument('--root-ca', default="AmazonRootCA1.pem", help="File path to root certificate authority, in PEM format. " +
                                      "Necessary if MQTT server uses a certificate that's not already in " +
                                      "your trust store.")
parser.add_argument('--client-id', default="test-" + str(uuid4()), help="Client ID for MQTT connection.")
parser.add_argument('--topic1', default="llenados", help="Topic to subscribe to, and publish messages to.")
parser.add_argument('--topic2', default="incidentes", help="Topic to subscribe to, and publish messages to.")
parser.add_argument('--message', default="Hello World!", help="Message to publish. " +
                                                              "Specify empty string to publish nothing.")
parser.add_argument('--count', default=0, type=int, help="Number of messages to publish/receive before exiting. " +
                                                          "Specify 0 to run forever.")
parser.add_argument('--use-websocket', default=False, action='store_true',
    help="To use a websocket instead of raw mqtt. If you " +
    "specify this option you must specify a region for signing.")
parser.add_argument('--signing-region', default='us-east-1', help="If you specify --use-web-socket, this " +
    "is the region that will be used for computing the Sigv4 signature")
parser.add_argument('--proxy-host', help="Hostname of proxy to connect to.")
parser.add_argument('--proxy-port', type=int, default=8080, help="Port of proxy to connect to.")
parser.add_argument('--verbosity', choices=[x.name for x in io.LogLevel], default=io.LogLevel.NoLogs.name,
    help='Logging level')

# Using globals to simplify sample code
args = parser.parse_args()

io.init_logging(getattr(io.LogLevel, args.verbosity), 'stderr')

received_count = 0
received_all_event = threading.Event()

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    if received_count == args.count:
        received_all_event.set()

if __name__ == '__main__':
    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    proxy_options = None
    if (args.proxy_host):
        proxy_options = http.HttpProxyOptions(host_name=args.proxy_host, port=args.proxy_port)

    if args.use_websocket == True:
        credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)
        mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=args.endpoint,
            client_bootstrap=client_bootstrap,
            region=args.signing_region,
            credentials_provider=credentials_provider,
            http_proxy_options=proxy_options,
            ca_filepath=args.root_ca,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=30)

    else:
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=args.endpoint,
            port=args.port,
            cert_filepath=args.cert,
            pri_key_filepath=args.key,
            client_bootstrap=client_bootstrap,
            ca_filepath=args.root_ca,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=30,
            http_proxy_options=proxy_options)

    print("Connecting to {} with client ID '{}'...".format(args.endpoint, args.client_id))

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    if args.message:
        if args.count == 0:
            print ("Sending messages until program killed")
        else:
            print ("Sending {} message(s)".format(args.count))
    
        publish_count = 1

        while (publish_count <= args.count) or (args.count == 0):

            # conexion modbus
            #client.connect()
            # obtener arreglo de datos del PLC [1er equipo]
            #rh = client.read_holding_registers(602, 5, unit=UNIT)

            # asignar valores obtenidos para llenados
            porcentaje = 50
            presion = 1 #rh.registers[2]
            temperatura = 2 #int(rh.registers[3] / 10)
            estatus_bomba = 1
            ciclos = 3  #rh.registers[4]
            cilindro = 4    #rh.registers[1]
            equipo = 1
            operador = 1002

            # asignar valores obtenidos para alarma_tipos
            alarma = PA #rh.registers[0]
            estatus = 1
            equipo = 1

            messageLlenados = "{\"porcentaje\" : \"" + str(porcentaje) + "\", " + \
                              "\"presion\" : \"" + str(presion) + "\", " + \
                              "\"temperatura\" : \"" + str(temperatura) + "\", " + \
                              "\"estatus_bomba\" : \"" + str(estatus_bomba) + "\", " + \
                              "\"ciclos\" : \"" + str(ciclos) + "\", " + \
                              "\"cilindro\" : \"" + str(cilindro) + "\", " + \
                              "\"equipo\" : \"" + str(equipo) + "\", " + \
                              "\"operador\" : \"" + str(operador) + "\"}"
            #print("args.message: " + messageLlenados)

            messageIncidentes = "{\"estatus\": \"" + str(estatus) + "\", " + \
                                "\"equipo\": \"" + str(equipo) + "\", " + \
                                "\"alarma\": \"" + alarma + "\"}"
            #print("args.message: " + messageIncidentes)

            """
            # obtener fecha y hora actual
            now = datetime.now()
            dt_now = now.strftime("%Y-%m-%d %H:%M:%S")
            fecha = dt_now
            """

            if temp_presion != presion or temp_temperatura != temperatura or temp_porcentaje != porcentaje or temp_ciclos != ciclos:
                # cambiar valores temporales al detectar cambio
                temp_presion = presion
                temp_temperatura = temperatura
                temp_porcentaje = porcentaje
                temp_ciclos = ciclos

                print("Publishing message to topic '{}': {}".format(args.topic, messageLlenados))
                message_json = json.dumps(messageLlenados)
                mqtt_connection.publish(
                    topic=args.topic1,
                    payload=message_json,
                    qos=mqtt.QoS.AT_LEAST_ONCE)
                time.sleep(1)
                publish_count += 1
            else:
                print("Sin cambios en llenados")

            if alarma != 0:
                #temp_alarma = alarma

                print("Publishing message to topic '{}': {}".format(args.topic, messageIncidentes))
                message_json = json.dumps(messageIncidentes)
                mqtt_connection.publish(
                    topic=args.topic2,
                    payload=message_json,
                    qos=mqtt.QoS.AT_LEAST_ONCE)
                time.sleep(1)
                publish_count += 1
            else:
                print("Sin cambios en alarmas")

            # cierre de conexión modbus
            #client.close()

            # retraso de ejecución en segundos
            time.sleep(5)

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

#python3 pubsub.py --root-ca ~/Certs/AmazonRootCA1.pem --cert ~/Certs/af895ae3f3926b675823a573f233c8ec4722b7e508bc987f96e3234249f7880b-certificate.pem.crt --key ~/Certs/af895ae3f3926b675823a573f233c8ec4722b7e508bc987f96e3234249f7880b-private.pem.key --endpoint a19egjpzgi9ikd-ats.iot.us-west-1.amazonaws.com