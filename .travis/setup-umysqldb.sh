pip install -U pymysql
pip install git+https://github.com/NextThought/umysqldb.git#egg=umysqldb
export RS_MY_DRIVER=umysqldb
python -c 'import relstorage.adapters.mysql.drivers as D; print(D.preferred_driver_name,D.driver_map)'
`dirname $0`/mysql.sh
