wget https://dev.mysql.com/get/Downloads/Connector-Python/mysql-connector-python-2.1.5.tar.gz
tar -xf mysql-connector-python-2.1.5.tar.gz
cd ./mysql-connector-python-2.1.5
python ./setup.py install --with-mysql-capi=/usr
`dirname $0`/mysql.sh
