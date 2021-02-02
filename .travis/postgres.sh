RS_PG_UNAME=${RELSTORAGETEST_PG_UNAME:-postgres}
psql -U $RS_PG_UNAME $RELSTORAGETEST_PG_DBNAME -c "CREATE USER relstoragetest WITH PASSWORD 'relstoragetest';"
psql -U $RS_PG_UNAME $RELSTORAGETEST_PG_DBNAME -c "CREATE DATABASE relstoragetest OWNER relstoragetest;"
psql -U $RS_PG_UNAME $RELSTORAGETEST_PG_DBNAME -c "CREATE DATABASE relstoragetest2 OWNER relstoragetest;"
psql -U $RS_PG_UNAME $RELSTORAGETEST_PG_DBNAME -c "CREATE DATABASE relstoragetest_hf OWNER relstoragetest;"
psql -U $RS_PG_UNAME $RELSTORAGETEST_PG_DBNAME -c "CREATE DATABASE relstoragetest2_hf OWNER relstoragetest;"
psql -U $RS_PG_UNAME $RELSTORAGETEST_PG_DBNAME -c "SELECT version();"
