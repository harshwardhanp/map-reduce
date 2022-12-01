# from google.cloud import storage
# import pickle
import subprocess
import random
import string
import pickle
from firebase_admin import credentials, initialize_app, storage
import multiprocessing

# Init firebase with your credentials
cred = credentials.Certificate("keystore.json")
initialize_app(cred, {'storageBucket': 'h-cluster-pool'})
letters = string.ascii_lowercase + string.digits

class cluster:
    def __init__(self) -> None:
        mappers = None
        reducers = None
        status = None
        __id = None
        __master = None
        __mappers  = None
        __controller  = None
        __reducers  = None
            
    def download_file(input_file_name, output_file_name):
        bucket = storage.bucket()
        blob = bucket.blob(input_file_name)   
        blob.download_to_filename(output_file_name)

    def upload_file(source_file, destination_file):
        bucket = storage.bucket()
        blob = bucket.blob(destination_file)   
        blob.upload_from_filename(source_file)

    def load_cluster_pool() -> list:

        blob_name = "h_cluster_pool.pkl"
        bucket = storage.bucket()
        blob = bucket.blob(blob_name)

        # reading python objects from a pickel file
        def pickle_loader():
            with blob.open("rb") as f:
                while True:
                    try:
                        yield pickle.load(f)
                    except EOFError:
                        break
        cluster_pool_list = []
        try:                    
            for cluster_list in pickle_loader():
                for cluster_obj in cluster_list:
                    cluster_pool_list.append(cluster_obj)
        except:
            pass
        
        return cluster_pool_list # returned the class objects of cluster

    def set_cluster_id(self):
        cluster_id = 'cls-'
        cluster_id += ''.join(random.choice(letters) for i in range(4))
        print("Cluster Id created", cluster_id)
        self.__id = cluster_id

    def get_cluster_id(self):
        return self.__id

    def set_master(self, master_data):
        self.__master_names = master_data[0]
        self.__master_ips = master_data[1]
        
        blob_name = self.get_cluster_id() + "_cluster_info.txt"
        with open(blob_name, 'w') as f: 
            for index, mp_name in enumerate(self.__master_names):
                f.write('%s:%s\n' % (self.__master_names[index], self.__master_ips[index]))
        
    def get_master(self):
        return self.__master_names , self.__master_ips

    def set_mappers(self, mapper_data):
        self.__mapper_names = mapper_data[0]
        self.__mapper_ips = mapper_data[1]

        blob_name = self.get_cluster_id() + "_cluster_info.txt"
        with open(blob_name, 'a') as f: 
            for index, mp_name in enumerate(self.__mapper_names):
                f.write('%s:%s\n' % (self.__mapper_names[index], self.__mapper_ips[index]))

    def get_mappers(self):
        return self.__mapper_names, self.__mapper_ips

    def set_controller(self, controller_data):
        self.__controller_names = controller_data[0]
        self.__controller_ips = controller_data[1]

    def get_controller(self):
        return self.__controller_names, self.__controller_ips

    def set_reducers(self, reducer_data):
        self.__reducer_names = reducer_data[0]
        self.__reducer_ips = reducer_data[1]

        blob_name = self.get_cluster_id() + "_cluster_info.txt"

        with open(blob_name, 'a') as f:
            for index, mp_name in enumerate(self.__reducer_names):
                f.write('%s:%s\n' % (self.__reducer_names[index], self.__reducer_ips[index]))

    def get_reducers(self):
        return self.__reducer_names, self.__reducer_ips
    
    def create_network(self, cluster_id, machine_count):
        temp_file = open("temp.txt",'w')
        subprocess.call(["bash","create_network.sh",cluster_id, str(machine_count)], stdout=temp_file)
        with open("temp.txt",'r') as file:
            output = file.read()
        print(output)
        temp_file.close()

    def delete_network(self, cluster_id, machine_count):
        temp_file = open("temp.txt",'w')
        subprocess.call(["bash","delete_network.sh",cluster_id], stdout=temp_file)
        with open("temp.txt",'r') as file:
            output = file.read()
        print(output)
        temp_file.close()

    def create_vms(self, cluster_id, spec, count):
        vm_names = []
        vm_ips = []
        for c in range(count):
            temp_file = open("temp.txt",'w')
            machine_name = cluster_id + '-' + spec.lower() + '-' + str(c)
            subprocess.call(["bash","create_vm.sh",machine_name, cluster_id], stdout=temp_file)
            with open("temp.txt",'r') as file:
                output = file.read()
            vm_names.append(machine_name)
            vm_ips.append(output.split()[-1])
        return vm_names, vm_ips

    def stop_vm(instance_name):
        temp_file = open("temp.txt",'w')
        subprocess.call(["bash","stop_vm.sh",str(instance_name)], stdout=temp_file)
        with open("temp.txt",'r') as file:
            output = file.read()
        print(output)
        temp_file.close()

    def start_vm(instance_name):
        temp_file = open("temp.txt",'w')
        subprocess.call(["bash","start_vm.sh",str(instance_name)], stdout=temp_file)
        with open("temp.txt",'r') as file:
            output = file.read()
        print(output)
        temp_file.close()

    def delete_vm(instance_name):
        temp_file = open("temp.txt",'w')
        subprocess.call(["bash","delete_vm.sh",str(instance_name)], stdout=temp_file)
        with open("temp.txt",'r') as file:
            output = file.read()
        print(output)
        temp_file.close()

    def set_status(self, status):
        self.__status = status

    def get_status(self):
        return self.__status

    def upload_details(self):
        
        blob_name = self.get_cluster_id() + "_cluster_info.txt"
        bucket = storage.bucket()
        blob = bucket.blob(blob_name)   
        blob.upload_from_filename(blob_name)

    def create_cluster(cls_obj, mappers, reducers) -> str:
        #create cluster
        cls_obj.set_cluster_id()
        cluster_id = cls_obj.get_cluster_id()

        cls_obj.create_network( cluster_id, mappers+reducers+2)

        cls_obj.set_master(cls_obj.create_vms(cluster_id, 'Master', 1))  #return only one ip
        print("Master Created")
        
        cls_obj.set_mappers(cls_obj.create_vms(cluster_id, 'Mapper', mappers)) # return list of ips
        print("Mappers Created")
        
        cls_obj.set_reducers(cls_obj.create_vms(cluster_id, 'Reducer', reducers)) #return list of ips
        print("Reducers created")

        cls_obj.upload_details()
        print("Completed Uploading details in the cloud")
        
        cls_obj.set_status("Created")

        return cluster_id

    def init_cluster(cls_obj):
        print("initialising cluster : ", cls_obj.get_cluster_id())
        mapper_names, mapper_ips = cls_obj.get_mappers()
        reducer_names, reducer_ips = cls_obj.get_reducers()

        cls_obj.set_status("Running")
        
        for mpr in mapper_names:
            subprocess.call(["bash","init_vm.sh",str(mpr), "mapper_server"])
        print("All mappers initialised")
        for rdcr in reducer_names:
            subprocess.call(["bash","init_vm.sh",str(rdcr), "reducer_server"])
        print("All reducers initialised")
        return cls_obj.get_status()
    
    def get_cluster_ids(cluster_pool):
        ids = []
        for cluster_obj in cluster_pool:
            ids.append(cluster_obj.__id)
        
        return ids
  
    def get_cluster_obj(cluster_id, cluster_pool):
        for cluster_obj in cluster_pool:
            if cluster_id == cluster_obj.__id:
                return cluster_obj
    
    def set_mapper_func(self, mapper):
        self.__mapper_func = mapper
        return True

    def get_mapper_func(self):
        return self.__mapper_func

    def set_reducer_func(self, reducer):
        self.__reducer_func = reducer
        return True

    def get_reducer_func(self):
        return self.__reducer_func

    def set_input_file(self, input_file):
        self.__input_file = input_file
        return True

    def get_input_file(self):
        return self.__input_file
    
    def set_output_file(self, output_file):
        self.__output_file = output_file
        return True
    
    def get_output_file(self):
        return self.__output_file

    def run_mapred(cls_obj):
        cluster_id = cls_obj.get_cluster_id()
        # output_file = cls_obj.create_master(cluster_id, 'Master')
        
        master_name = cls_obj.get_master()[0][0]
        subprocess.call(["bash","run_master.sh",master_name, cluster_id, cls_obj.get_mapper_func() ,cls_obj.get_reducer_func(), cls_obj.get_input_file(), cls_obj.get_output_file()])
        
        cloud_output_location = str(cluster_id) + '/fnfle-' + cls_obj.get_output_file()
        output_file = cls_obj.get_output_file()
        cls_obj.download_file(cloud_output_location, output_file)
        return output_file

    def destroy_cluster(cls_obj):
        pass

    def update_cluster_pool(cluster_pool):
        
        # project_id="harshwardhan-patil-fall2022"

        # storage_client = storage.Client(project=project_id)
        # buckets = storage_client.list_buckets()
        # names_of_buckets = []
        # for bucket in buckets:
        #     names_of_buckets.append(bucket.name)

        # bucket_name = "h-cluster-pool"
        # blob_name = "h_cluster_pool.pkl"

        # if bucket_name not in names_of_buckets:
        #     storage_client.create_bucket(bucket_name)
        #     # print("Bucket created.")

        # bucket = storage_client.bucket(bucket_name)
        blob_name = "h_cluster_pool.pkl"
        bucket = storage.bucket()
        blob = bucket.blob(blob_name)
                
        with blob.open('wb') as outp:
            pickle.dump(cluster_pool, outp, pickle.HIGHEST_PROTOCOL)

    def exit():
        print("Exited")
