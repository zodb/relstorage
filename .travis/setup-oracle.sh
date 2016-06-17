pip install -U -e ".[oracle]"
sqlplus 'sys/oracle@localhost/orcl' as sysdba <<EOF
    CREATE USER relstoragetest IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest;
    CREATE USER relstoragetest2 IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest2;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest2;
    CREATE USER relstoragetest_hf IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest_hf;
    CREATE USER relstoragetest2_hf IDENTIFIED BY relstoragetest;
    GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO relstoragetest2_hf;
    GRANT EXECUTE ON DBMS_LOCK TO relstoragetest2_hf;

    grant unlimited tablespace to relstoragetest;
    grant unlimited tablespace to relstoragetest2;
    grant unlimited tablespace to relstoragetest_hf;
    grant unlimited tablespace to relstoragetest2_hf;
EOF
