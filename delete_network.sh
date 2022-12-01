yes Yes | gcloud compute firewall-rules delete $1-allow-ssh
yes Yes | gcloud compute firewall-rules delete $1-allow-internal
yes Yes | gcloud compute networks delete $1