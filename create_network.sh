gcloud compute networks create $1
gcloud compute firewall-rules create $1-allow-ssh --network $1 --allow tcp:22,icmp --source-ranges 0.0.0.0/0
gcloud compute firewall-rules create $1-allow-internal --network $1 --allow tcp:0-65535,udp:0-65535,icmp --source-ranges 10.128.0.0/9