import datetime
import socket
from _thread import start_new_thread
import sys
import firebase_admin
import pickle
from firebase_admin import credentials, initialize_app, storage
import os 
from map import map_function

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
        
    def perform_map(self, hostname, cluster_id, client_connection):
        try:
            print("WC Mapper initialised")
            status = 'in-progress'
            input_data_file = cluster_id + '/' + hostname + '-mpinput.txt'
            output_data_file = cluster_id + '-' + hostname + '-mpinput.txt'
            self.download_file(input_data_file, output_data_file)
            print("downloaded mapper input")
            data_file = open(output_data_file, 'r')
            file_text = data_file.read()
            input_data = file_text.split()

            print("splitted input data")
            mapper_output = map_function(output_data_file, input_data)
            
            print("map completed")
            number_of_reducers = self.get_reducer_data(cluster_id)
            print("got number of reducers")

            output_file = [0] * number_of_reducers
            for i in range(number_of_reducers):
                output_file[i] = open(str(cluster_id + '-m-' + hostname + '-' + str(i + 1) + '.txt'), "w+")
            
            print("created mapper output files")
            
            for kvpair in mapper_output:
                reducer_num = hash(kvpair[0]) % number_of_reducers
                output_file[reducer_num].write(str(kvpair[0]) + ' ' + str(kvpair[1]) + '\n')

            print("separate output files created and data is written on each of them")
            for i in range(number_of_reducers):
                output_file[i].close()

            for i in range(number_of_reducers):
                mp_opfile_localname = cluster_id + '-m-' + hostname + '-' + str(i + 1) + '.txt'
                mp_opfile_uploadname = cluster_id + '/m-' + hostname + '-' + str(i + 1) + '.txt'
                self.upload_file(mp_opfile_localname, mp_opfile_uploadname )

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
            if command[0] == "init_map":
                print("Map initialised")
                cluster_id ,function_name = command[1:]
                if function_name == "map_wc":
                    
                    self.perform_map(hostname, cluster_id, client_connection)

                elif function_name == "map_ini":
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
