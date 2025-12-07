-- ===============================
-- Experiment 1: MySQL (Hash vs BTree)
-- Using 10^5 dataset from DBTuneSuite
-- ===============================

-- Create and select DB
CREATE DATABASE IF NOT EXISTS employees_index;
USE employees_index;

-- -------------------------------------------------
-- Load the full 10^5 employees CSV into InnoDB
-- -------------------------------------------------
DROP TABLE IF EXISTS employees_full;

CREATE TABLE employees_full (
    ssnum INT PRIMARY KEY,
    name  VARCHAR(100),
    dept  VARCHAR(100)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE '/home/sr7463/DBTuneSuite/data_generation/employees/employeesindex_10_5.csv'
INTO TABLE employees_full
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM employees_full;

-- -------------------------------------------------
-- Build dept_map: map dept strings → small int IDs
-- (Used ONLY to shrink MEMORY table so it can fit 10^5 rows)
-- -------------------------------------------------

DROP TABLE IF EXISTS dept_map;

CREATE TABLE dept_map (
    dept_id INT AUTO_INCREMENT PRIMARY KEY,
    dept    VARCHAR(100) UNIQUE
) ENGINE=InnoDB;

INSERT INTO dept_map (dept)
SELECT DISTINCT dept
FROM employees_full
ORDER BY dept;

SELECT COUNT(*) FROM dept_map;
SELECT * FROM dept_map LIMIT 10;

-- -------------------------------------------------
-- MEMORY table for Hash vs BTree experiment
-- NOTE: dept_id INT replaces dept VARCHAR(100)
-- to fit full 10^5 rows inside MEMORY engine.
-- -------------------------------------------------

DROP TABLE IF EXISTS employees_mem;

CREATE TABLE employees_mem (
    ssnum   INT,
    dept_id INT
) ENGINE=MEMORY;

INSERT INTO employees_mem (ssnum, dept_id)
SELECT f.ssnum, m.dept_id
FROM employees_full AS f
JOIN dept_map      AS m
  ON f.dept = m.dept;

SELECT COUNT(*) FROM employees_mem;

-- =================================================
-- M1: MULTIPOINT QUERY (equality) on dept_id = 1
-- Compare BTREE vs HASH
-- =================================================

-- BTREE
-- clean any old dept indexes (ignore errors if they appear)
DROP INDEX idx_dept_btree ON employees_mem;
DROP INDEX idx_dept_hash  ON employees_mem;

CREATE INDEX idx_dept_btree ON employees_mem (dept_id) USING BTREE;

SET profiling = 1;
SELECT * FROM employees_mem WHERE dept_id = 1;
SHOW PROFILES;

-- HASH
DROP INDEX idx_dept_btree ON employees_mem;

CREATE INDEX idx_dept_hash ON employees_mem (dept_id) USING HASH;

SET profiling = 1;
SELECT * FROM employees_mem WHERE dept_id = 1;
SHOW PROFILES;

-- =================================================
-- M2: POINT QUERY (equality) on ssnum = 15000
-- Compare BTREE vs HASH
-- =================================================

-- BTREE
-- clean old ssnum indexes (ignore errors if they appear)
DROP INDEX idx_ssnum_btree ON employees_mem;
DROP INDEX idx_ssnum_hash  ON employees_mem;

CREATE INDEX idx_ssnum_btree ON employees_mem (ssnum) USING BTREE;

SET profiling = 1;
SELECT * FROM employees_mem WHERE ssnum = 15000;
SHOW PROFILES;

-- HASH
DROP INDEX idx_ssnum_btree ON employees_mem;

CREATE INDEX idx_ssnum_hash ON employees_mem (ssnum) USING HASH;

SET profiling = 1;
SELECT * FROM employees_mem WHERE ssnum = 15000;
SHOW PROFILES;

-- =================================================
-- M3: RANGE QUERY on ssnum
-- Compare BTREE vs HASH for range [10000, 20000]
-- =================================================

-- BTREE
DROP INDEX idx_ssnum_hash ON employees_mem;

CREATE INDEX idx_ssnum_btree ON employees_mem (ssnum) USING BTREE;

SET profiling = 1;
SELECT * FROM employees_mem
WHERE ssnum BETWEEN 10000 AND 20000;
SHOW PROFILES;

-- HASH
DROP INDEX idx_ssnum_btree ON employees_mem;

CREATE INDEX idx_ssnum_hash ON employees_mem (ssnum) USING HASH;

SET profiling = 1;
SELECT * FROM employees_mem
WHERE ssnum BETWEEN 10000 AND 20000;
SHOW PROFILES;

-- =================================================
-- M4: RANGE QUERY on dept_id
-- Compare BTREE vs HASH for range [1, 100]
-- =================================================

-- BTREE
DROP INDEX idx_dept_hash ON employees_mem;

CREATE INDEX idx_dept_btree ON employees_mem (dept_id) USING BTREE;

SET profiling = 1;
SELECT * FROM employees_mem
WHERE dept_id BETWEEN 1 AND 100;
SHOW PROFILES;

-- HASH
DROP INDEX idx_dept_btree ON employees_mem;

CREATE INDEX idx_dept_hash ON employees_mem (dept_id) USING HASH;

SET profiling = 1;
SELECT * FROM employees_mem
WHERE dept_id BETWEEN 1 AND 100;
SHOW PROFILES;

-- End of experiment script

-- To run: SOURCE home/sr7463/q2_hash_btree_mysql.sql;
