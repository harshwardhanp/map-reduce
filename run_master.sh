# username, cluster_id, mapper_func, reducer_func, input_file, output_file
gcloud compute ssh $1 --zone=us-west1-a  --command="python3 /home/mapreduce/master.py ${USER} ${2} ${3} ${4} ${5} ${6} &"