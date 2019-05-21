# To be able to successfully test against a MySQL on a different host,
# it's best to set up a proxy: socat tcp-listen:3306,reuseaddr,fork tcp:192.168.99.100:3306
# This can arise when running mysql in docker on macOS, which doesn't
# correctly handle the `docker run` `-p` option to forward ports to the local host;
# specify the address of the virtual machine.
# docker run  --publish 3306:3306 --rm --name mysqld -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.0
HOST="-h ${RS_DB_HOST-localhost}"
mysql -uroot $HOST -e "CREATE USER 'relstoragetest' IDENTIFIED BY 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE relstoragetest;"
mysql -uroot $HOST -e "GRANT ALL ON relstoragetest.* TO 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE relstoragetest2;"
mysql -uroot $HOST -e "GRANT ALL ON relstoragetest2.* TO 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE relstoragetest_hf;"
mysql -uroot $HOST -e "GRANT ALL ON relstoragetest_hf.* TO 'relstoragetest';"
mysql -uroot $HOST -e "CREATE DATABASE relstoragetest2_hf;"
mysql -uroot $HOST -e "GRANT ALL ON relstoragetest2_hf.* TO 'relstoragetest';"
mysql -uroot $HOST -e "FLUSH PRIVILEGES;"
