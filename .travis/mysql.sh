# To be able to successfully test against a MySQL on a different host,
# it's best to set up a proxy: socat tcp-listen:3306,reuseaddr,fork tcp:192.168.99.100:3306
# This can arise when running mysql in docker on macOS, which doesn't
# correctly handle the `docker run` `-p` option to forward ports to the local host;
# specify the address of the virtual machine.
# docker run  --publish 3306:3306 --rm --name mysqld -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.0
HOST="-h ${RS_DB_HOST-localhost}"
DBNAME=${RELSTORAGETEST_DBNAME:-relstoragetest}
DBNAME2=${DBNAME}2
DBNAME_HF=${DBNAME}_hf
DBNAME2_HF=${DBNAME}2_hf
echo $DBNAME_hf
echo $DBNAME2_hf
mysql -uroot $HOST -e "CREATE USER 'relstoragetest' IDENTIFIED BY 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE $DBNAME;"
mysql -uroot $HOST -e "GRANT ALL ON $DBNAME.* TO 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE $DBNAME2;"
mysql -uroot $HOST -e "GRANT ALL ON $DBNAME2.* TO 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE $DBNAME_HF;"
mysql -uroot $HOST -e "GRANT ALL ON $DBNAME_HF.* TO 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE $DBNAME2_HF;"
mysql -uroot $HOST -e "GRANT ALL ON $DBNAME2_HF.* TO 'relstoragetest';"
mysql -uroot $HOST -e "GRANT SELECT ON performance_schema.* TO 'relstoragetest'"
mysql -uroot $HOST -e "GRANT SELECT ON sys.* TO 'relstoragetest'"
mysql -uroot $HOST -e "GRANT PROCESS ON *.* TO 'relstoragetest'"
mysql -uroot $HOST -e "SELECT version()"
mysql -uroot $HOST -e "FLUSH PRIVILEGES;"
