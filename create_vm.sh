MY_INSTANCE_NAME=$1
ZONE=us-west1-a 
SUBNET_NAME=$2

gcloud compute instances create $MY_INSTANCE_NAME \
    --image-family=debian-10 \
    --image-project=debian-cloud \
    --machine-type=g1-small \
    --metadata-from-file startup-script=startup-script.sh \
    --zone $ZONE \
    --network-interface=network-tier=PREMIUM,subnet=$SUBNET_NAME

# gcloud compute instances create $MY_INSTANCE_NAME \
#     --image-family=debian-10 \
#     --image-project=debian-cloud \
#     --machine-type=g1-small \
#     --metadata-from-file startup-script=startup-script.sh \
#     --zone $ZONE \
#     --network-interface=network-tier=PREMIUM,subnet=$SUBNET_NAME

# new instance create script
# gcloud compute instances create $MY_INSTANCE_NAME \
#     --network-interface=network-tier=PREMIUM,subnet=$subnet_name \
#     --metadata-from-file startup-script=startup-script.sh \
#     --zone $ZONE \
#     --tags http-server
# gcloud compute scp keystore.json $USER@$1:/home/$USER --zone=us-west1-a

gcloud compute instances describe $1 --zone=us-west1-a --format='get(networkInterfaces[0].networkIP)'


# gcloud compute instances create server-vm --metadata-from-file startup-script=startup-script.sh --zone=us-central1-a --network-interface=network-tier=PREMIUM,subnet=harsh-ecc-fall22
# gcloud compute instances create client-vm --metadata-from-file startup-script=startup-script.sh --zone=us-central1-a --network-interface=network-tier=PREMIUM,subnet=harsh-ecc-fall22
