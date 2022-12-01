#python3 master.py cluster_id
import socket
import multiprocessing
import time
import firebase_admin
import pickle
from firebase_admin import credentials, initialize_app, storage
import sys
import splitter
import os
import combiner
from reduce import reduce_function

def send(message, function_name, cluster_id, host_ip):
    host = host_ip
    port = 3278
    reply_msg=""

    try:
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((host, port))
    except socket.error as e:
        print("failed")

    try:
        response = clientSocket.recv(1024).decode()
        print("Response from client", response)
        if response != "Success":
            raise Exception("Error Occured in Mapredce phase")
        print(message)
        print(cluster_id)
        print(function_name)
        print(host_ip)
        command = message + " " + cluster_id +" "+ function_name
        print(command)
        clientSocket.send(command.encode())

        Response = clientSocket.recv(1024).decode("utf-8")
        if Response != "Completed":
            raise Exception("Error Occured in Mapredce phase")
        clientSocket.send(str.encode("exit"))
        clientSocket.close()

        return 0
    except Exception as ex:
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
    local_input_file_for_R = str(cluster_identifier) + '-r-' + str(reducer_identifier) + '-rdinput.txt'
    cloud_input_file_for_R = str(cluster_identifier) + '/r-' + str(reducer_identifier) + '-rdinput.txt'
    
    input_file = open(local_input_file_for_R, "w+")
    for mapper in mapper_list:
        mp_opfile_cloudname = cluster_identifier + '/m-' + mapper + '-' + str(reducer_number) + '.txt'
        mp_opfile_localname = cluster_identifier + '-m-' + mapper + '-' + str(reducer_number) + '.txt'
        download_file(mp_opfile_cloudname, mp_opfile_localname )

    # mp_opfile_uploadname = cluster_id + '/m-' + hostname + '-' + str(i + 1) + '.txt'
    for mapper in mapper_list:
        mp_opfile_localname = cluster_identifier + '-m-' + mapper + '-' + str(reducer_number) + '.txt'
        with open(mp_opfile_localname, "r") as mapper_output:
            input_file.write(mapper_output.read())

    input_file.close()
    upload_file(local_input_file_for_R, cloud_input_file_for_R)


def reducer_output_combiner(cluster_identifier, reducer_list, output_file_name):
    local_rdoutput_combiner = str(cluster_identifier) + '-r-rdoutput-combined.txt'
    # cloud_rdoutput_combiner = str(cluster_identifier) + '/r-rdoutput-combined.txt'
    


    for reducer in reducer_list:
        mp_opfile_cloudname = cluster_identifier + '/' + reducer + '-rdoutput.txt'
        mp_opfile_localname = cluster_identifier + '-' + reducer + '-rdoutput.txt'
        download_file(mp_opfile_cloudname, mp_opfile_localname )
    
    rdoutput_combiner = open(local_rdoutput_combiner, "w+")
    
    # mp_opfile_uploadname = cluster_id + '/m-' + hostname + '-' + str(i + 1) + '.txt'
    for reducer in reducer_list:
        reducer_r_output_file = cluster_identifier + '-' + reducer + '-rdoutput.txt'
        with open(reducer_r_output_file, "r") as reducer_output:
            rdoutput_combiner.write(reducer_output.read())

    rdoutput_combiner.close()

    data_file = open(local_rdoutput_combiner, "r")
    
    file_lines = data_file.readlines()

    reducer_output = reduce_function(file_lines)

    local_output_location = str(cluster_identifier) + '-fnfle-' + output_file_name
    cloud_output_location = str(cluster_identifier) + '/fnfle-' + output_file_name
    
    output_file = open(local_output_location, "w+")
    for key, value in reducer_output.items():
        output_file.write(str(key) + ' ' + str(value) + '\n')
    output_file.close()
    upload_file(local_output_location, cloud_output_location)
    return cloud_output_location

def filter_text(dirty_file):
    data_file = open(dirty_file, 'r', encoding='utf-8')
    dirty_text = data_file.read()

    killpunctuation = str.maketrans('', '', r"()\"“”’#/@;:<>{}[]*-=~|.?,0123456789")

    clean_text = dirty_text.translate(killpunctuation)

    clean_file_name = 'clean_'+dirty_file
    fp = open(clean_file_name, 'w+', encoding='utf-8')
    fp.write(clean_text.lower())
    fp.close()
    return clean_file_name

def input_data_splitter(cluster_identifier, mapper_identifiers, input_file):
    number_of_mappers = len(mapper_identifiers)
    data_file = open(filter_text(input_file), 'r')
    file_text = data_file.read()
    input_data = file_text.split()
    data_file.close()
    bucket = storage.bucket()

    # if not os.path.exists(cluster_identifier):
    #     os.mkdir(cluster_identifier)

    for i in range(number_of_mappers):
        output_file_name = str(cluster_identifier) + '/' + str(mapper_identifiers[i]) + '-mpinput.txt'
        output_file = open( output_file_name , "w+")
        data = input_data[round(i * (len(input_data)/number_of_mappers)):round((i+1) * (len(input_data)/number_of_mappers))]
        for word in data:
            output_file.write(word+'\n')
        output_file.close()
        blob = bucket.blob(output_file_name)   
        blob.upload_from_filename(output_file_name)

# print(send("127.0.1.1",1432,"get Harsh"))
# 1. get list of arguments - clusterid, mapper_function, reducer_function
# 2. get the cluster info from cluster id
# 3. separate mapper ips and reducer ips, keep them in a list
# 4. separate input file for m mappers call splitter function for that
# thus create input files for mappers and reducers using their host name
# 5. parallely iterate over the mappers indicating start function
# mention input_file for that mapper_________this will be mapper_hostname.txt , list of reducer_hostnames


 
# total arguments
# n = len(sys.argv)
# print("Total arguments passed:", n)
 
# Arguments passed
try:
    if len(sys.argv) != 6:
        raise Exception("Invalud argument number")

    username, cluster_id, mapper_func, reducer_func, input_file, output_file= sys.argv[1:]
    # username = sys.argv[1]
    certificate_file_path = "home/"+username+"/keystore.json"
    
    # Init firebase with your credentials
    cred = credentials.Certificate(certificate_file_path)
    initialize_app(cred, {'storageBucket': 'h-cluster-pool'})
    
    cluster_info_filename = cluster_id + "_cluster_info.txt"
    # uncomment
    bucket = storage.bucket()
    blob = bucket.blob(cluster_info_filename)   
    
    #Create a cluster_directory if cluster directory does not exist
    if not os.path.exists(cluster_id):
        os.mkdir(cluster_id)

    list_machines = []
    # uncomment
    with blob.open("r") as f:
        list_machines = f.readlines()

    # comment
    # with open(cluster_info_filename, "r") as f:
    #     list_machines = f.readlines()
    
    # print(list_machines)
    mapper = {}
    reducer = {}

    for line in list_machines:
        name_ip = line.split(":")
        if cluster_id + "-mapper-" in name_ip[0]: 
            mapper[name_ip[0]] = name_ip[1]
        if cluster_id + "-reducer-" in name_ip[0]: 
            reducer[name_ip[0]] = name_ip[1]

    mappers = len(mapper)

    blob = bucket.blob(input_file)   
    blob.download_to_filename(input_file)
    
    input_data_splitter(cluster_id, mapper.keys(), input_file)

    def mapper_init():
        while True:
            pool = multiprocessing.Pool(processes=mappers)
            outputs = pool.starmap(send , [("init_map", mapper_func, cluster_id, value) for value in mapper.values()])

            if sum(outputs) == 0:
                break
    
    mapper_init()

    for key, index in enumerate(reducer.keys()):
        mapper_output_combiner(cluster_id, index + 1, key, mapper.keys())

    def reducer_init():
        while True:
            reducers = len(reducer)
            pool = multiprocessing.Pool(processes=reducers)
            outputs = pool.starmap(send , [("init_reduce", reducer_func, cluster_id, value) for value in reducer.values()])
            
            if sum(outputs) == 0:
                break


    reducer_init()

    cloud_output_location = reducer_output_combiner(cluster_id, reducer.keys(), output_file)

    print(cloud_output_location)
except Exception as ex:
    print(ex)