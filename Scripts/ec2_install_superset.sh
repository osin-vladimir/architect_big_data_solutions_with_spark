sudo apt-get install build-essential libssl-dev libffi-dev python-dev python-pip libsasl2-dev libldap2-dev
pip3 install cryptography
pip3 install virtualenv

mkdir /root/superset
cd /root/superset/
virtualenv venv_superset
source /root/superset/venv_superset/bin/activate
pip3 install --upgrade setuptools pip
pip3 install superset
fabmanager create-admin --app superset
superset db upgrade
superset load_examples
superset init

