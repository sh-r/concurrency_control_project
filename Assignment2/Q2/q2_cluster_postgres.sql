-- ==========================================
-- Experiment 2: PostgreSQL (Index vs No Index)
-- Mirroring MySQL clustered-like scenarios
-- Using 10^5 rows from employees
-- ==========================================

\c employees_index

\pset pager off
\timing on

-- ------------------------------------------
-- 0. Build a fresh table for this experiment
-- ------------------------------------------

DROP TABLE IF EXISTS employees_cluster_pg;

CREATE TABLE employees_cluster_pg AS
SELECT
    ssnum,
    name,
    -- same derived columns as in MySQL small experiment
    (ssnum * 13) % 100000          AS range_uniform,
    FLOOR(ssnum / 1000)::INT       AS range_skew,
    CASE WHEN ssnum % 10 = 0
         THEN 1
         ELSE ssnum
    END                            AS hot_col
FROM employees;

SELECT COUNT(*) AS rows_in_experiment
FROM employees_cluster_pg;

SELECT
    MIN(range_uniform), MAX(range_uniform),
    MIN(range_skew),    MAX(range_skew),
    MIN(hot_col),       MAX(hot_col)
FROM employees_cluster_pg;

-- ============================================================
-- S1: RANGE QUERY (UNIFORM DISTRIBUTION)
--     Indexed ssnum vs indexed range_uniform vs no index
-- ============================================================

DROP INDEX IF EXISTS idx_pg_ssnum;
DROP INDEX IF EXISTS idx_pg_range_uniform;

CREATE INDEX idx_pg_ssnum
ON employees_cluster_pg (ssnum);

CREATE INDEX idx_pg_range_uniform
ON employees_cluster_pg (range_uniform);

-- Cluster-like range query on ssnum
SELECT COUNT(*) FROM employees_cluster_pg
WHERE ssnum BETWEEN 10000 AND 20000;

-- Non-clustered-like range query on range_uniform (with index)
SELECT COUNT(*) FROM employees_cluster_pg
WHERE range_uniform BETWEEN 10000 AND 20000;

-- No-index on range_uniform: drop its index only
DROP INDEX IF EXISTS idx_pg_range_uniform;

SELECT COUNT(*) FROM employees_cluster_pg
WHERE range_uniform BETWEEN 10000 AND 20000;

-- ============================================================
-- S2: POINT QUERY (UNIFORM DISTRIBUTION)
--     Indexed ssnum vs indexed range_uniform vs no index
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_pg_ssnum
ON employees_cluster_pg (ssnum);

CREATE INDEX idx_pg_range_uniform
ON employees_cluster_pg (range_uniform);

-- Cluster-like point query on ssnum
SELECT COUNT(*) FROM employees_cluster_pg
WHERE ssnum = 50000;

-- Non-clustered-like point query on range_uniform (with index)
SELECT COUNT(*) FROM employees_cluster_pg
WHERE range_uniform = 50000;

-- No-index point on range_uniform
DROP INDEX IF EXISTS idx_pg_range_uniform;

SELECT COUNT(*) FROM employees_cluster_pg
WHERE range_uniform = 50000;

-- ============================================================
-- S3: RANGE QUERY ON SKEWED DISTRIBUTION (range_skew)
--     With index vs No index
-- ============================================================

DROP INDEX IF EXISTS idx_pg_range_skew;

CREATE INDEX idx_pg_range_skew
ON employees_cluster_pg (range_skew);

SELECT COUNT(*) FROM employees_cluster_pg
WHERE range_skew BETWEEN 10 AND 20;

DROP INDEX IF EXISTS idx_pg_range_skew;

SELECT COUNT(*) FROM employees_cluster_pg
WHERE range_skew BETWEEN 10 AND 20;

-- ============================================================
-- S4: MULTIPOINT HOTSPOT QUERY (hot_col = 1)
--     With index vs No index
-- ============================================================

DROP INDEX IF EXISTS idx_pg_hot_col;

CREATE INDEX idx_pg_hot_col
ON employees_cluster_pg (hot_col);

SELECT COUNT(*) FROM employees_cluster_pg
WHERE hot_col = 1;

DROP INDEX IF EXISTS idx_pg_hot_col;

SELECT COUNT(*) FROM employees_cluster_pg
WHERE hot_col = 1;

-- End of PostgreSQL clustered-like index experiment
