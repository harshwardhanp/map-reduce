gcloud compute firewall-rules delete $1-allow-ssh
gcloud compute firewall-rules delete $1-allow-internal
gcloud compute networks delete $1