
Running Tests
=============

To run these tests, you need to create a test user account and several
databases. Use or adapt the SQL statements below to create the
databases.

PostgreSQL
----------

CREATE USER relstoragetest WITH PASSWORD 'relstoragetest';
CREATE DATABASE relstoragetest OWNER relstoragetest;
CREATE DATABASE relstoragetest2 OWNER relstoragetest;
CREATE DATABASE relstoragetest_hf OWNER relstoragetest;
CREATE DATABASE relstoragetest2_hf OWNER relstoragetest;

Also, add the following lines to the top of pg_hba.conf (if you put
them at the bottom, they may be overridden by other parameters):

local   relstoragetest     relstoragetest   md5
local   relstoragetest2    relstoragetest   md5
local   relstoragetest_hf  relstoragetest   md5
local   relstoragetest2_hf relstoragetest   md5


MySQL
-----

CREATE USER 'relstoragetest'@'localhost' IDENTIFIED BY 'relstoragetest';
CREATE DATABASE relstoragetest;
GRANT ALL ON relstoragetest.* TO 'relstoragetest'@'localhost';
CREATE DATABASE relstoragetest2;
GRANT ALL ON relstoragetest2.* TO 'relstoragetest'@'localhost';
CREATE DATABASE relstoragetest_hf;
GRANT ALL ON relstoragetest_hf.* TO 'relstoragetest'@'localhost';
CREATE DATABASE relstoragetest2_hf;
GRANT ALL ON relstoragetest2_hf.* TO 'relstoragetest'@'localhost';
FLUSH PRIVILEGES;
