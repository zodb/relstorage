wget https://dev.mysql.com/get/Downloads/Connector-Python/mysql-connector-python-8.0.6.tar.gz
tar -xf mysql-connector-python-8.0.6.tar.gz
cd ./mysql-connector-python-8.0.6
python ./setup.py install --with-mysql-capi=/usr
cd ..
python -c 'import relstorage.adapters.mysql.drivers as D; print(D.preferred_driver_name,D.driver_map)'
`dirname $0`/mysql.sh
