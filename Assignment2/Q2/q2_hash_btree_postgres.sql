-- ==========================================
-- Experiment 1: PostgreSQL (Hash vs BTree)
-- Using 10^5 employeesindex_10_5.csv dataset
-- ==========================================

\c employees_index

\pset pager off
\timing on

-- ==========================================
-- P1: MULTIPOINT equality query on hundreds1
--     Compare BTREE vs HASH
--     Query: SELECT count(*) FROM employees WHERE hundreds1 = 100;
-- ==========================================

-- BTREE on hundreds1
DROP INDEX IF EXISTS idx_hundreds1_btree;
DROP INDEX IF EXISTS idx_hundreds1_hash;

CREATE INDEX idx_hundreds1_btree
ON employees USING btree (hundreds1);

SELECT count(*) FROM employees
WHERE hundreds1 = 100;

-- HASH on hundreds1
DROP INDEX IF EXISTS idx_hundreds1_btree;

CREATE INDEX idx_hundreds1_hash
ON employees USING hash (hundreds1);

SELECT count(*) FROM employees
WHERE hundreds1 = 100;

-- ==========================================
-- P2: POINT equality query on ssnum
--     Compare BTREE vs HASH
--     Query: SELECT count(*) FROM employees WHERE ssnum = 15000;
-- ==========================================

-- BTREE on ssnum
DROP INDEX IF EXISTS idx_ssnum_btree;
DROP INDEX IF EXISTS idx_ssnum_hash;

CREATE INDEX idx_ssnum_btree
ON employees USING btree (ssnum);

SELECT count(*) FROM employees
WHERE ssnum = 15000;

-- HASH on ssnum
DROP INDEX IF EXISTS idx_ssnum_btree;

CREATE INDEX idx_ssnum_hash
ON employees USING hash (ssnum);

SELECT count(*) FROM employees
WHERE ssnum = 15000;

-- ==========================================
-- P3: RANGE query on ssnum
--     Compare BTREE vs HASH for range [10000, 20000]
--     Query: SELECT count(*) FROM employees WHERE ssnum BETWEEN 10000 AND 20000;
-- ==========================================

-- BTREE on ssnum
DROP INDEX IF EXISTS idx_ssnum_hash;

CREATE INDEX idx_ssnum_btree
ON employees USING btree (ssnum);

SELECT count(*) FROM employees
WHERE ssnum BETWEEN 10000 AND 20000;

-- HASH on ssnum
DROP INDEX IF EXISTS idx_ssnum_btree;

CREATE INDEX idx_ssnum_hash
ON employees USING hash (ssnum);

SELECT count(*) FROM employees
WHERE ssnum BETWEEN 10000 AND 20000;

-- ==========================================
-- P4: RANGE query on hundreds1
--     Compare BTREE vs HASH for range [100, 200]
--     Query: SELECT count(*) FROM employees WHERE hundreds1 BETWEEN 100 AND 200;
-- ==========================================

-- BTREE on hundreds1
DROP INDEX IF EXISTS idx_hundreds1_hash;

CREATE INDEX idx_hundreds1_btree
ON employees USING btree (hundreds1);

SELECT count(*) FROM employees
WHERE hundreds1 BETWEEN 100 AND 200;

-- HASH on hundreds1
DROP INDEX IF EXISTS idx_hundreds1_btree;

CREATE INDEX idx_hundreds1_hash
ON employees USING hash (hundreds1);

SELECT count(*) FROM employees
WHERE hundreds1 BETWEEN 100 AND 200;

-- End of experiment script

-- To run:
-- cd /home/sr7463
-- psql -h /home/sr7463/pgsql/data/run -p 10001 -U sr7463 -d employees_index -f q2_hash_btree_postgres.sql
