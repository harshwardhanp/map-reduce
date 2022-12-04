# Map-Reduce on Cloud with VMs and firebase

We know the popular Map-Reduce paper. This is the basic implementation of map-reduce using the virtual machines as master, mappers and reducers with Google Firestore as a backend system.

## Installation

Along with python3 and gcloud, use the package manager [pip](https://pip.pypa.io/en/stable/) to install required libraries. They are mentioned under requirements.txt as well.

```bash
pip install socket
pip install multiprocessing 
pip install pickle
pip install firebase_admin
```

## Instructions to run the program
#### 0. Running a program
To initiate the cluster console use following command.
```
python3 main.py
```

#### 1. Creating cluster
Syntax :
```
cls-create -m no-of-mappers -r no-of-reducers
```
This command will create a network with unique identifier inside google cloud. Also, the virtual machine instances of a master, mappers, and reducers is created with its own specifications. 

This command will give you a newly generated cluster id.

#### 2. Initializing a cluster
Syntax :
```
cls-init -id cluster-id
```
This command will transfer the necessary files to the cloud and then will return status of the cluster as running.



#### 3. Setting map and reduce functions for the cluster
Syntax :
```
cls-set-mapred -id cluster-id -m mapper_function -r reducer_function
```
This command will set the mapper and reducer function to the cluster to execute. At this moment, our mappers only support "map_wc" function and reducers only support "red_wc" for getting word counts. As we proceed in future, we plan to add more functionality to the mappers.



#### 4. Running map and reduce functionality
Syntax :
```
cls-run-mapred -id cluster-id -i input_file_name -o output_file_name
```
This command will run the mapper and reducer functionality on the cluster. You have to specify the input file location, output file location with cluster-id.

#### 5. Destroying a cluster
Syntax :
```
cls-destroy -id cluster-id
```
Once a map-reduce is over and the cluster-status is completed, you can destroy the cluster using this command. It will delete all the master, mapper and reducer VM instances along with the related network and the firewall rules. Give it couple of minutes to complete


#### 6. Reading output for a cluster
Syntax :
```
cls-read-output -id cluster-id
```
This command will read output for a cluster with the help of cluster-id

#### 7. Reading a file using file-name
Syntax : 
```
cls-read-file -f file-name
```
This command will read output file for a mentioned file name


#### 8. Getting the Cluster status
##### &emsp; a. For all cluster  
Syntax : 
```
cls-status -all
```
##### &emsp; b. For specific cluster  
Syntax : 
```
cls-status -id cluster-id
```

#### 9. Exiting from cluster
Syntax : 
```
cls-exit
```
You will be exited from the cluster-console.



## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

#### If you happened to encounter an error in your application, do mail it to mail@harshwardhanpatil.com
