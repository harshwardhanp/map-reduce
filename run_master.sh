# master_name,  cluster_id, mapper_func , reducer_func, input_file, output_file
gcloud compute ssh $1 --zone=us-central1-a  --command="python3 master.py ${USER} ${2} ${3} ${4} ${5} ${6}" &