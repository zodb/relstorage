RS_PG_UNAME=${RELSTORAGETEST_PG_UNAME:-postgres}
psql -U $RS_PG_UNAME -c "CREATE USER relstoragetest WITH PASSWORD 'relstoragetest';"
psql -U $RS_PG_UNAME -c "CREATE DATABASE relstoragetest OWNER relstoragetest;"
psql -U $RS_PG_UNAME -c "CREATE DATABASE relstoragetest2 OWNER relstoragetest;"
psql -U $PS_PG_UNAME -c "CREATE DATABASE relstoragetest_hf OWNER relstoragetest;"
psql -U $RS_PG_UNAME -c "CREATE DATABASE relstoragetest2_hf OWNER relstoragetest;"
psql -U $RS_PG_UNAME -c "SELECT version();"
