echo 'Download python file to local temp'
aws s3 cp s3://space-pipeline/src/download.py /tmp/download.py
aws s3 cp s3://space-pipeline/src/requirements.txt /tmp/requirements.txt
# Install python on Ubuntu
sudo yum -y install epel-release
sudo yum -y install python36
sudo easy_install-3.6 pip
pip3 install --user -r /tmp/requirements.txt
python36 /tmp/download.py
