# Install or update needed software
apt-get update
apt-get install -yq git supervisor python python-pip python3-distutils
pip install --upgrade pip virtualenv

sudo curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
sudo python3 get-pip.py

# Fetch source code
export HOME=/root
git clone https://github.com/GoogleCloudPlatform/getting-started-python.git /opt/app
git clone https://github.com/harshwardhanp/map-reduce.git /home/mapreduce/
# Install Cloud Ops Agent
sudo bash /opt/app/gce/add-google-cloud-ops-agent-repo.sh --also-install

# Account to own server process
useradd -m -d /home/pythonapp pythonapp

# Python environment setup
virtualenv -p python3 /opt/app/gce/env
/bin/bash -c "source /opt/app/gce/env/bin/activate"
/opt/app/gce/env/bin/pip install -r /opt/app/gce/requirements.txt
pip3 install -r /home/mapreduce/requirements.txt

# Set ownership to newly created account
chown -R pythonapp:pythonapp /opt/app

# Put supervisor configuration in proper place
cp /opt/app/gce/python-app.conf /etc/supervisor/conf.d/python-app.conf

# Start service via supervisorctl
supervisorctl reread
supervisorctl update