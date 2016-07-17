pip install -U pymysql
pip install git+https://github.com/NextThought/umysqldb.git#egg=umysqldb
export RS_MY_DRIVER=umysqldb
python -c 'import relstorage.adapters._mysql_drivers as D; print(D.preferred_driver_name,D.driver_map)'
`dirname $0`/mysql.sh
