#python3 master.py cluster_id
import socket
import multiprocessing
import time
import firebase_admin
import pickle
from firebase_admin import credentials, initialize_app, storage
import sys
from reduce import reduce_function

username = sys.argv[1]
certificate_file_path = "/home/"+username+"/keystore.json"
# certificate_file_path = "keystore.json"

# Init firebase with your credentials
cred = credentials.Certificate(certificate_file_path)
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})

def send(message, function_name, cluster_id, host_ip):
    host = host_ip[:-1]
    port = 3278
    reply_msg=""
    try:
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((host, port))
        response = clientSocket.recv(1024).decode()
        print("Response from client", response)
        if response != "Success":
            raise Exception("Error Occured in Mapredce phase while establishing connection")
        command = message + " " + cluster_id +" "+ function_name
        clientSocket.send(command.encode())
        Response = clientSocket.recv(1024).decode("utf-8")
        if Response != "Completed":
            raise Exception("Error Occured in Mapredce phase while working with "+ function_name)
        clientSocket.send(str.encode("exit"))
        clientSocket.close()
        return 0
    except Exception as ex:
        print(ex)
        return 1


def download_file(input_file_name, output_file_name):
    bucket = storage.bucket()
    blob = bucket.blob(input_file_name)   
    blob.download_to_filename(output_file_name)


def upload_file(source_file, destination_file):
    bucket = storage.bucket()
    blob = bucket.blob(destination_file)   
    blob.upload_from_filename(source_file)


def mapper_output_combiner(cluster_identifier, reducer_number, reducer_identifier, mapper_list):
    bucket = storage.bucket()
    cloud_input_file_for_R = str(cluster_identifier) + '/r-' + str(reducer_identifier) + '-rdinput.txt'
    CIR = bucket.blob(cloud_input_file_for_R)   
    with CIR.open("w") as input_file:
        for mapper in mapper_list:
            mp_opfile_localname = cluster_identifier + '/m-' + mapper + '-' + str(reducer_number) + '.txt'
            MOL = bucket.blob(mp_opfile_localname)   
            with MOL.open("r") as mapper_output:
                input_file.write(mapper_output.read())


def reducer_output_combiner(cluster_identifier, reducer_list, output_file_name):
    bucket = storage.bucket()
    cloud_rdoutput_combiner = str(cluster_identifier) + '/r-rdoutput-combined.txt'
    CRC = bucket.blob(cloud_rdoutput_combiner)   
    with CRC.open("w") as rdoutput_combiner:
        for reducer in reducer_list:
            reducer_r_output_file = cluster_identifier + '/' + reducer + '-rdoutput.txt'
            RROFL = bucket.blob(reducer_r_output_file)   
            with RROFL.open("r") as reducer_output:
                rdoutput_combiner.write(reducer_output.read())
    with CRC.open("r") as data_file:
        file_lines = data_file.readlines()
        reducer_output = reduce_function(file_lines)
        cloud_output_location = str(cluster_identifier) + '/fnfle-' + output_file_name
        COL = bucket.blob(cloud_output_location)   
        with COL.open("w") as output_file:
            for key, value in reducer_output.items():
                output_file.write(str(key) + ' ' + str(value) + '\n')
    return cloud_output_location


def filter_text(cluster_identifier, dirty_file):
    bucket = storage.bucket()
    cls_dirty_file = cluster_identifier+'/'+dirty_file
    DFL = bucket.blob(cls_dirty_file)   
    clean_file_name = cluster_identifier+'/'+'clean_'+dirty_file
    with DFL.open('r', encoding='utf-8') as data_file:
        dirty_text = data_file.read()
        killpunctuation = str.maketrans('', '', r"()\"“”’#/@;:<>{}[]*-=~|.?,0123456789")
        clean_text = dirty_text.translate(killpunctuation)
        CFN = bucket.blob(clean_file_name)
        with CFN.open('w', encoding='utf-8') as fp:
            fp.write(clean_text.lower())
    return clean_file_name


def input_data_splitter(cluster_identifier, mapper_identifiers, input_file):
    bucket = storage.bucket()
    number_of_mappers = len(mapper_identifiers)
    clean_file_name = filter_text(cluster_identifier, input_file)
    CFN = bucket.blob(clean_file_name)
    input_data = []
    with CFN.open('r') as data_file:
        file_text = data_file.read()
        input_data = file_text.split()
    print("got input data")
    for  i, mapper_name in enumerate(mapper_identifiers):
        output_file_name = str(cluster_identifier) + '/' + str(mapper_name) + '-mpinput.txt'
        OFN = bucket.blob(output_file_name)
        with OFN.open("w") as output_file:
            start = round(i * len(input_data)/number_of_mappers)
            end = round((i+1) * int(len(input_data)/number_of_mappers))
            data = input_data[start:end]
            for word in data:
                output_file.write(word+'\n')


try:
    if len(sys.argv) != 7:
        raise Exception("Invalud argument number")

    username, cluster_id, mapper_func, reducer_func, input_file, output_file= sys.argv[1:]
    cluster_info_filename = cluster_id + "_cluster_info.txt"
    bucket = storage.bucket()
    blob = bucket.blob(cluster_info_filename)   
    list_machines = []
    with blob.open("r") as f:
        list_machines = f.readlines()

    mapper = {}
    reducer = {}
    for line in list_machines:
        name_ip = line.split(":")
        if cluster_id + "-mapper-" in name_ip[0]: 
            mapper[name_ip[0]] = name_ip[1]
        if cluster_id + "-reducer-" in name_ip[0]: 
            reducer[name_ip[0]] = name_ip[1]
            
    mappers = len(mapper)
    input_data_splitter(cluster_id, mapper.keys(), input_file)

    # def mapper_init():
    #     while True:
    #         pool = multiprocessing.Pool(processes=mappers)
    #         outputs = pool.starmap(send , [("init_map", mapper_func, cluster_id, value) for value in mapper.values()])
    #         if sum(outputs) == 0:
    #             break
    # mapper_init()
    for value in mapper.values():
        send("init_map", mapper_func, cluster_id, value)

    time.sleep(30)

    for index, key in enumerate(reducer.keys()):
        mapper_output_combiner(cluster_id, index + 1, key, mapper.keys())

    # def reducer_init():
    #     while True:
    #         reducers = len(reducer)
    #         pool = multiprocessing.Pool(processes=reducers)
    #         outputs = pool.starmap(send , [("init_reduce", reducer_func, cluster_id, value) for value in reducer.values()])
    #         if sum(outputs) == 0:
    #             break
    # reducer_init()
    for value in reducer.values():
        send("init_reduce", reducer_func, cluster_id, value)
    time.sleep(30)
    

    cloud_output_location = reducer_output_combiner(cluster_id, reducer.keys(), output_file)

    print(cloud_output_location)
except Exception as ex:
    print(ex)