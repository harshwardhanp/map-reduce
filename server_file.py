import datetime
import socket
from _thread import start_new_thread
import sys

class Server:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.thread_count = 0
        self.filename = 'keyValueStorage.txt'

    # Usage : takes file name and key which has to be searched in the file and
    # returns the latest key-value pair in the file.
    # if not found : returns ERROR
    def find_key_in_file(self, filename, key):
        with open(filename, 'r') as target_file:
            list_of_lines = []
            for num, line in enumerate(target_file.readlines(), 1):
                # print(line)
                if str(key) == line.split()[0]:
                    list_of_lines.append(line.strip())
        return list_of_lines

    def set_data(self, storage_file, key_value_string):
        try:
            file = open(storage_file, "a")
            # print('\n' + key_value_string)
            get_words_in_line = key_value_string.strip().split()
            key = get_words_in_line[0]
            flag1 = int(get_words_in_line[1])
            flag2 = int(get_words_in_line[2])

            block_size = int(get_words_in_line[3])

            value_for_key = ' '.join(map(str, get_words_in_line[4:]))
            if int(block_size) < len(value_for_key.encode()):
                raise MemoryError
            file.write('\n' + key_value_string)
            return 'STORED'
        except MemoryError as me:
            return 'NOT-STORED : Memory overflow (value-size-bytes are less than actual bytes of value )'
        except ValueError as ve:
            return 'NOT-STORED : ' + ve.args[0]
        except:
            return 'NOT-STORED : Internal Error Occurred'
        finally:
            file.close()

    # Usage : This function takes the filename, key and the connection object
    # and then sends the fetched key value pair or the error message to the client
    def get_data(self, storage_file, key_string, connection):
        get_lines = self.find_key_in_file(storage_file, key_string)

        if len(get_lines) != 0:
            get_words_in_line = get_lines[-1].strip().split()
            key_and_blocksize = 'VALUE ' + get_words_in_line[0] + ' ' + get_words_in_line[3] + ' '
            value_for_key = ' '.join(map(str, get_words_in_line[4:]))
            connection.send((key_and_blocksize + '\r\n' + value_for_key + '\r\n' + 'END').encode())
            # connection.send(value_for_key.encode())

        else:
            connection.send('ERROR : Value for requested key not found'.encode())

    def connect_to_client(self, client_connection):
        client_connection.send('Connection established with the Server'.encode())

        while True:
            data = client_connection.recv(1024).decode()
            command = data.strip().split()

            if command[0] == 'get' and len(command) == 2:
                self.get_data(self.filename, command[1], client_connection)

            elif command[0] == 'set' and len(command) >= 6:
                send_line = self.set_data(self.filename, data[4:])
                client_connection.send(send_line.encode())

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

    # server_ip='0.0.0.0'
    # with open('host-ip.txt', 'r') as iplist:
    #     input_ip_list = iplist.readlines()
    #     server_ip=input_ip_list[0].rstrip()
    #     port = int(input_ip_list[1].rstrip())
    # Python Program to Get IP Address
    
    hostname = socket.gethostname()
    server_ip = socket.gethostbyname(hostname)
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

        start_new_thread(server.connect_to_client, (client,))
        server.thread_count += 1
        print(datetime.datetime.now(), "\t Client Connected Number :  " + str(server.thread_count))
