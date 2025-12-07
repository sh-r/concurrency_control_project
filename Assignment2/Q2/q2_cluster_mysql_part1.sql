-- ==========================================
-- Experiment 2: MySQL (Clustered vs Non-clustered vs No Index)
-- Using 10^5 rows from employees_full
-- ==========================================

USE employees_index;

-- ------------------------------------------
-- 0. Build a fresh table for this experiment
--    so we don't modify employees_full directly
-- ------------------------------------------

DROP TABLE IF EXISTS employees_cluster;

CREATE TABLE employees_cluster (
    ssnum INT PRIMARY KEY,  -- clustered index
    name  VARCHAR(100),
    dept  VARCHAR(100)
) ENGINE=InnoDB;

INSERT INTO employees_cluster (ssnum, name, dept)
SELECT ssnum, name, dept
FROM employees_full;

SELECT COUNT(*) AS rows_in_employees_cluster
FROM employees_cluster;

-- ------------------------------------------
-- 1. Add extra columns to control "distribution"
--    range_uniform : arithmetic permutation of ssnum (uniform-ish)
--    range_skew    : low-domain, skewed distribution
--    hot_col       : hotspot value for multipoint query
-- ------------------------------------------

ALTER TABLE employees_cluster
  ADD COLUMN range_uniform INT,
  ADD COLUMN range_skew    INT,
  ADD COLUMN hot_col       INT;

-- range_uniform: permutation of ssnum (keeps uniform-ish spread)
UPDATE employees_cluster
SET range_uniform = (ssnum * 13) MOD 100000;

-- range_skew: many rows share each value (domain 0..100)
UPDATE employees_cluster
SET range_skew = FLOOR(ssnum / 1000);

-- hot_col: 10% of rows have value = 1 (hotspot)
UPDATE employees_cluster
SET hot_col = IF(ssnum % 10 = 0, 1, ssnum);

SELECT MIN(range_uniform), MAX(range_uniform),
       MIN(range_skew),    MAX(range_skew)
FROM employees_cluster;

-- Turn on profiling for timing
SET profiling = 1;

-- ============================================================
-- C1: POINT QUERY, UNIFORM DISTRIBUTION
--     Clustered vs Non-clustered vs No Index
--
--     Queries:
--       Clustered:   WHERE ssnum = 50000
--       Non-cluster: WHERE range_uniform = 50000 (with index)
--       No-index:    WHERE range_uniform = 50000 (no index)
-- ============================================================

-- ---- Clustered point query (PRIMARY KEY = clustered index)
SELECT * FROM employees_cluster
WHERE ssnum = 50000;
SHOW PROFILES;

-- ---- Non-clustered point query (index on range_uniform)

CREATE INDEX idx_range_uniform
ON employees_cluster (range_uniform);

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE range_uniform = 50000;
SHOW PROFILES;

-- ---- No-index point query on range_uniform (full scan)

DROP INDEX idx_range_uniform ON employees_cluster;

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE range_uniform = 50000;
SHOW PROFILES;

-- ============================================================
-- C2: RANGE QUERY, UNIFORM DISTRIBUTION
--     Clustered vs Non-clustered vs No Index
--
--     Queries:
--       Clustered:   ssnum BETWEEN 10000 AND 20000
--       Non-cluster: range_uniform BETWEEN 10000 AND 20000 (with index)
--       No-index:    range_uniform BETWEEN 10000 AND 20000 (no index)
-- ============================================================

-- ---- Clustered range query (PRIMARY KEY range)

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE ssnum BETWEEN 10000 AND 20000;
SHOW PROFILES;

-- ---- Non-clustered range query (index on range_uniform)

CREATE INDEX idx_range_uniform
ON employees_cluster (range_uniform);

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE range_uniform BETWEEN 10000 AND 20000;
SHOW PROFILES;

-- ---- No-index range query on range_uniform

DROP INDEX idx_range_uniform ON employees_cluster;

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE range_uniform BETWEEN 10000 AND 20000;
SHOW PROFILES;

-- ============================================================
-- C3: RANGE QUERY, SKEWED DISTRIBUTION
--     Non-clustered vs No Index on range_skew
--
--     range_skew has small domain (0..100), many rows per value.
--     This shows how index benefit changes under skew.
--
--     Queries:
--       With index: range_skew BETWEEN 10 AND 20
--       No index:   range_skew BETWEEN 10 AND 20
-- ============================================================

-- ---- Non-clustered range query on skewed column (with index)

CREATE INDEX idx_range_skew
ON employees_cluster (range_skew);

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE range_skew BETWEEN 10 AND 20;
SHOW PROFILES;

-- ---- Same range query with NO index

DROP INDEX idx_range_skew ON employees_cluster;

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE range_skew BETWEEN 10 AND 20;
SHOW PROFILES;

-- ============================================================
-- C4: MULTIPOINT QUERY WITH HOTSPOT
--     Non-clustered vs No Index on hot_col
--USE employees_index;

-- Fresh small experiment table
DROP TABLE IF EXISTS employees_cluster_small;

CREATE TABLE employees_cluster_small (
    ssnum INT PRIMARY KEY,   -- clustered index
    name  VARCHAR(100),
    dept  VARCHAR(100),
    range_uniform INT
) ENGINE=InnoDB;

INSERT INTO employees_cluster_small (ssnum, name, dept)
SELECT ssnum, name, dept
FROM employees_full;

-- range_uniform: permutation of ssnum
UPDATE employees_cluster_small
SET range_uniform = (ssnum * 13) MOD 100000;

SELECT COUNT(*) AS rows_in_experiment
FROM employees_cluster_small;

SET profiling = 1;

-- ================================
-- S1: RANGE QUERY
-- Clustered vs Non-clustered vs No index
-- ================================

-- Clustered range query on PRIMARY KEY (ssnum)
SELECT COUNT(*) FROM employees_cluster_small
WHERE ssnum BETWEEN 10000 AND 20000;

-- Non-clustered range query (with index on range_uniform)
CREATE INDEX idx_range_uniform ON employees_cluster_small (range_uniform);

SELECT COUNT(*) FROM employees_cluster_small
WHERE range_uniform BETWEEN 10000 AND 20000;

-- No-index range query (drop index)
DROP INDEX idx_range_uniform ON employees_cluster_small;

SELECT COUNT(*) FROM employees_cluster_small
WHERE range_uniform BETWEEN 10000 AND 20000;

-- ================================
-- S2: POINT QUERY
-- Clustered vs Non-clustered vs No index
-- ================================

-- Clustered point query
SELECT COUNT(*) FROM employees_cluster_small
WHERE ssnum = 50000;

-- Non-clustered point query (with index)
CREATE INDEX idx_range_uniform ON employees_cluster_small (range_uniform);

SELECT COUNT(*) FROM employees_cluster_small
WHERE range_uniform = 50000;

-- No-index point query
DROP INDEX idx_range_uniform ON employees_cluster_small;

SELECT COUNT(*) FROM employees_cluster_small
WHERE range_uniform = 50000;

-- At the very end, show all profiles
SHOW PROFILES;
--     hot_col = 1 for ~10% of rows, else = ssnum.
--     This is a classic hotspot multipoint query.
--
--     Queries:
--       With index: hot_col = 1
--       No index:   hot_col = 1
-- ============================================================

-- ---- Non-clustered multipoint query (with index on hot_col)

CREATE INDEX idx_hot_col
ON employees_cluster (hot_col);

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE hot_col = 1;
SHOW PROFILES;

-- ---- Same multipoint query with NO index

DROP INDEX idx_hot_col ON employees_cluster;

SET profiling = 1;
SELECT * FROM employees_cluster
WHERE hot_col = 1;
SHOW PROFILES;

-- End of clustered vs non-clustered experiment script
