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
certificate_file_path = "home/"+username+"/keystore.json"

# Init firebase with your credentials
cred = credentials.Certificate(certificate_file_path)
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})


class Server:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.thread_count = 0

    def download_file(self, input_file_name, output_file_name):
        bucket = storage.bucket()
        blob = bucket.blob(input_file_name)   
        blob.download_to_filename(output_file_name)
    
    def upload_file(self, source_file, destination_file):
        bucket = storage.bucket()
        blob = bucket.blob(destination_file)   
        blob.upload_from_filename(source_file)

    def get_reducer_data(self, cluster_id):
        cluster_info_filename = cluster_id + "_cluster_info.txt"
        self.download_file(cluster_info_filename, cluster_info_filename)   
        

        list_machines = []
        with open(cluster_info_filename,"r") as f:
            list_machines = f.readlines()

        reducer_names = []
        for line in list_machines:
            name_ip = line.split(":")
            if cluster_id + "-reducer-" in name_ip[0]: 
                reducer_names.append(name_ip[0])
        return len(reducer_names)
        
    def perform_reduce(self, hostname, cluster_id, client_connection):
        try:
            print("WC reducer initialised")
            status = 'in-progress'
            input_data_file = cluster_id + '/r-' + hostname + '-rdinput.txt'
            output_data_file = cluster_id + '-r-' + hostname + '-rdinput.txt'
            self.download_file(input_data_file, output_data_file)
            print("downloaded reducer input")
            data_file = open(output_data_file, 'r')
            file_lines = data_file.readlines()
            reducer_output = reduce_function(file_lines)
            
            cloud_output_file = str(cluster_id) + '/' + str(hostname) + '-rdoutput.txt'
            local_output_file = str(cluster_id) + '-' + str(hostname) + '-rdoutput.txt'
            output_file = open(local_output_file, "w+")
            for key, value in reducer_output.items():
                output_file.write(str(key) + ' ' + str(value) + '\n')
            output_file.close()
            self.upload_file(local_output_file, cloud_output_file )

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
  
    hostname ="cls-f8ly-reducer-0" #socket.gethostname()
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
