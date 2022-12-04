gcloud compute ssh $1 --zone=us-west1-a --command="python3 /home/mapreduce/${2}.py ${USER}" &
