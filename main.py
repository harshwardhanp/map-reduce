from cluster import cluster
from cls_help import cls_help
from pathlib import Path
class MainProgram:
    version = "1.0.0.01" 
    def __init__(self) -> None:
        pass

if __name__ == "__main__":
    print("Initialising H-Cluster console...")
    cluster_pool = []
    cluster_pool = cluster.load_cluster_pool()
    print("Successfully initialised H-Cluster with Version : ", MainProgram.version)
    while True:
        command = input("H-Cluster$")
        cmd_format = command.split()
        command_length = len(cmd_format)
        try:
            if command_length == 0:
                pass
            elif cmd_format[0] == "-version" and command_length == 1:
                print(MainProgram.version)  
            elif cmd_format[0] == "exit" and command_length == 1:
                print("Exiting from the H-cluster console...")
                break
            elif cmd_format[0] == "cls-help":
                if command_length == 1:
                    cls_help.getHelp()
                elif command_length == 2 and cmd_format[1] in cls_help.help_dict:
                    cls_help.getHelp(cmd_format[1])
                else:
                    raise Exception("Syntax Error!!! Please check syntax for cls-help command.")

            #cls-create -m 3 -r 3  -->> cls_id
            elif cmd_format[0] == "cls-create" and  command_length == 5   :
                if cmd_format[1] != "-m" and cmd_format[3] != "-r":
                    raise Exception("Syntax Error!!! Please check syntax for cls-create command.")
                    
                if not(cmd_format[2].isnumeric() and cmd_format[4].isnumeric()):
                    raise TypeError("Syntax Error!!! Mapper & Reducer numbers should be an Integer")

                number_of_mappers = int(cmd_format[2])
                number_of_reducers = int(cmd_format[4])
                
                cls_obj = cluster()
                cls_id = cluster.create_cluster(cls_obj, number_of_mappers, number_of_reducers)
                
                #This is where you are saving the current list of cluster.
                #you need to change this to get the cluster list 
                cluster_pool.append(cls_obj)

                cluster.update_cluster_pool(cluster_pool)
                
                print(cls_id)

            #cls-init -id cls_id -->> cls_id is running/failed/destroyed
            elif cmd_format[0] == "cls-init" :
                if command_length != 3 and cmd_format[1] != "-id":
                    raise Exception("Syntax Error!!! Please check syntax for cls-init command.")
                
                if cmd_format[2] not in cluster.get_cluster_ids(cluster_pool):
                    raise Exception("Command Error!!! Provided cluster not found in cluster pool")

                # do things
                cluster_id = cmd_format[2]
                # get the cluster object
                cls_obj = cluster.get_cluster_obj(cluster_id, cluster_pool)
                # Initialize cluster
                status = cluster.init_cluster(cls_obj)
                # check status if it is not running then something is failed
                if status != "Running":
                    raise RuntimeError("Error Occured while Initializing Cluster")

                # cluster.update_cluster_object()
                # cluster.update_cluster_pool(cluster_pool)
                print(cluster_id, 'Initiated..... Current status :: ', status)
                    
            #cls-set-mapred -id cls_id -m _mapper_ -r _reducer_
            elif cmd_format[0] == "cls-set-mapred" :
                if command_length != 7 and cmd_format[1] != "-id" and cmd_format[3] != "-m" and cmd_format[5] != "-r":
                    raise Exception("Syntax Error!!! Please check syntax for cls-set-mapred command.")
                
                if cmd_format[2] not in cluster.get_cluster_ids(cluster_pool):
                    raise Exception("Command Error!!! Provided cluster not found in cluster pool")
                
                # get the cluster object
                cluster_id = cmd_format[2]
                cls_obj = cluster.get_cluster_obj(cluster_id, cluster_pool)
                print("Got cluster object")
                mapper = cmd_format[4]  #map_wc #map_ini
                reducer = cmd_format[6] #red_wc #red_ini
                if mapper not in ['map_wc', 'map_ini']:
                    raise Exception("Unknown Mapper Function. Please provide valid mapper name: 'map_wc' or 'map_ini' ")
                print("Master has been set")
                if reducer not in ['red_wc','red_ini']:
                    raise Exception("Unknown Reducer Function. Please provide valid reducer name: 'red_wc' or 'red_ini' ")
                print("Reducer has been set")
                if not (cls_obj.set_mapper_func(mapper) and cls_obj.set_reducer_func(reducer)):
                    raise RuntimeError("Runtime Error!!! Error occured while setting up mapper and reducer")
                print("Both has been set")
                # cluster.update_cluster_object()
                # cluster.update_cluster_pool(cluster_pool)
            

            #cls-run-mapred -id cls_id -i _input_file_ -o _output_file_  -->> output file
            elif cmd_format[0] == "cls-run-mapred":
                if command_length != 2 and cmd_format[1] != "-id" and cmd_format[3] != "-i" and cmd_format[5] != "-o":
                    raise Exception("Syntax Error!!! Please chek syntax for cls-run-mapred command.")
                
                if cmd_format[2] not in cluster.get_cluster_ids(cluster_pool):
                    raise Exception("Command Error!!! Provided cluster not found in cluster pool")
                
                input_file_path = cmd_format[4]
                output_file = cmd_format[6]
                
                input_file = Path(input_file_path)
                if not input_file.is_file():
                    raise Exception("Input file Error!!! Please provide valid input file")
                try:
                    f = open(input_file,'r')
                    if not f.readable():
                        raise Exception("Input file Error!!! Input file is not readable")
                    f.close()
                except IOError as ioe:
                    raise Exception("Input file Error!!! Input file is not readable")
                
                cluster_id = cmd_format[2]
                cls_obj = cluster.get_cluster_obj(cluster_id, cluster_pool)

                if not (cls_obj.set_input_file(input_file_path) and cls_obj.set_output_file(output_file)):
                    raise RuntimeError("Runtime Error!!! Error occured while setting up mapper and reducer")
                print("Input and output files are set....  Running mapreduce")
                # running map reduce with
                # output_file = cluster.run_mapred(cls_obj)
                print("Map-Reduce successfully completed.... Output is stored in ", output_file)
                # cluster.update_cluster_object()
                # cluster.update_cluster_pool(cluster_pool)


            elif cmd_format[0] == "cls-read-file" and cmd_format[1] == "-f" and command_length == 3:
                output_file_path = cmd_format[2]
                output_file = Path(output_file_path)
                if not output_file.is_file():
                    raise Exception("File Error!!! Invalid file name")
                cluster.read_output_file(output_file_path)

            elif cmd_format[0] == "cls-read-output" and cmd_format[1] == "-id" and command_length == 3:
                if cmd_format[2] not in cluster.get_cluster_ids(cluster_pool):
                    raise Exception("Command Error!!! Provided cluster not found in cluster pool")
                cluster_id = cmd_format[2]
                cls_obj = cluster.get_cluster_obj(cluster_id, cluster_pool)

                output_file_path = cls_obj.get_output_file()
                output_file = Path(output_file_path)
                if not output_file.is_file():
                    raise Exception("File Error!!! Invalid file name")
                cluster.read_output_file(output_file_path)
            # cls-destroy -id cls_id
            elif cmd_format[0] == "cls-destroy":
                if command_length != 3 and cmd_format[1] != "-id":
                    raise Exception("Syntax Error!!! Please check syntax for cls-destroy command.")
                
                if cmd_format[2] not in cluster.get_cluster_ids(cluster_pool):
                    raise Exception("Command Error!!! Provided cluster not found in cluster pool")

                # do things
                cluster_id = cmd_format[2]
                # get the cluster object
                cls_obj = cluster.get_cluster_obj(cluster_id, cluster_pool)
                # Initialize cluster
                status = cluster.destroy_cluster(cls_obj)
                # check status if it is not running then something is failed
                if status != "Destroyed":
                    raise RuntimeError("Error Occured while Initializing Cluster")
                print(cluster_id, ' is ', status)
                # cluster.update_cluster_object()
                # cluster.update_cluster_pool(cluster_pool)

            # cls-status 
            elif cmd_format[0] == "cls-status":
                if command_length == 2 and cmd_format[1] == "-all":
                    cluster.get_all_status(cluster_pool)
                # cls-status -id cls_id  
                elif command_length == 3:
                    if cmd_format[1] != "-id":
                        raise Exception("Syntax Error!!! Please check syntax for cls-destroy command.")
                    
                    # get the cluster object
                    cluster_id = cmd_format[2]
                    cls_obj = cluster.get_cluster_obj(cluster_id, cluster_pool)

                    cluster.get_object_status(cls_obj)
                else:
                    raise Exception("Syntax Error!!! Please check syntax for cls-status command.")

            elif (cmd_format[0] == "cls-exit" or cmd_format[0] == "exit" ) and command_length == 1:
                
                confirmation = input("Execution of this command will exit the H-cluster console. Do you want to proceed?(Y/N)")
                if confirmation.lower() in ['y','yes']:
                    cluster.exit()
                    # cluster.update_cluster_object()
                    # cluster.update_cluster_pool(cluster_pool)
                    break
            else:
                print("Error Occured while processing the command \n Type cls-help to get the help")
        except RuntimeError as re:
            print(re)
        except Exception as ex:
            print(ex)