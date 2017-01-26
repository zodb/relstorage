pip install https://dev.mysql.com/get/Downloads/Connector-Python/mysql-connector-python-2.1.5.tar.gz
python -c 'import relstorage.adapters.mysql.drivers as D; print(D.preferred_driver_name,D.driver_map)'
`dirname $0`/mysql.sh
