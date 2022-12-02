import datetime
import socket
from _thread import start_new_thread
import sys
import firebase_admin
import pickle
from firebase_admin import credentials, initialize_app, storage
import os 
from reduce import reduce_function

username = sys.argv[1]
certificate_file_path = "/home/"+username+"/keystore.json"
# certificate_file_path = "keystore.json"

# Init firebase with your credentials
cred = credentials.Certificate(certificate_file_path)
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})


class Server:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.thread_count = 0
        
    def perform_reduce(self, hostname, cluster_id, client_connection):
        try:
            print("WC reducer initialised")
            status = 'in-progress'
            input_data_file = cluster_id + '/r-' + hostname + '-rdinput.txt'
            bucket = storage.bucket()
            IDF = bucket.blob(input_data_file)

            reducer_output = {}
            with IDF.open('r') as data_file:
                file_lines = data_file.readlines()
                reducer_output = reduce_function(file_lines) 
            
            cloud_output_file = str(cluster_id) + '/' + str(hostname) + '-rdoutput.txt'
            COF = bucket.blob(cloud_output_file)
            with COF.open("w") as output_file:
                for key, value in reducer_output.items():
                    output_file.write(str(key) + ' ' + str(value) + '\n')
            
            status = 'completed'
            client_connection.send('Completed'.encode())
        except Exception as e:
            status = 'failed'
            print(e)
            client_connection.send('failed'.encode())

    def connect_to_client(self, client_connection, hostname):
        client_connection.send('Success'.encode())

        while True:
            data = client_connection.recv(1024).decode()
            command = data.strip().split()
            print(command)
            if command[0] == "init_reduce":
                print("Reducer initialised")
                cluster_id ,function_name = command[1:]
                if function_name == "red_wc":
                    
                    self.perform_reduce(hostname, cluster_id, client_connection)

                elif function_name == "red_ini":
                    pass
                # client_connection.send('Completed'.encode())

            elif command[0] == 'exit':
                client_connection.close()
                self.thread_count -= 1
                print(datetime.datetime.now(),
                      "\t 1 client Dropped. Now, concurrent clients are :  " + str(self.thread_count))
                break
            else:
                send_line = 'Command Error. Please provide valid command'
                client_connection.send(send_line.encode())


if __name__ == '__main__':
  
    hostname = socket.gethostname()
    server_ip = socket.gethostbyname(socket.gethostname())
    port = 3278
    
    print("Your Computer Name is:" + hostname)
    print("Your Computer IP Address is:" + server_ip)

    server = Server(server_ip, port)
    try:
        ServerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ServerSocket.bind((server.host, server.port))
    except socket.error as e:
        print(datetime.datetime.now(), "\t Error occurred while initiating Server")
        print(str(e))

    print(datetime.datetime.now(), "\t Waitiing for client to establish connection ")
    ServerSocket.listen(5)  # At most 5 clients unacceptable connections allowed.

    while True:
        client, address = ServerSocket.accept()
        print(datetime.datetime.now(), "\t Connected to: " + address[0] + ":" + str(address[1]))

        start_new_thread(server.connect_to_client, (client,hostname,))
        server.thread_count += 1
        print(datetime.datetime.now(), "\t Client Connected Number :  " + str(server.thread_count))
