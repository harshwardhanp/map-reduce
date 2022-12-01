gcloud compute scp keystore.json $USER@$1:/home/$USER --zone=us-west1-a
gcloud compute ssh $1 --zone=us-west1-a --command="python3 /home/mapreduce/${2}.py ${USER}" &

