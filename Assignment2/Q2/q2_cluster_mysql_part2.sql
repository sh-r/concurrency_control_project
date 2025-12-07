USE employees_index;

-- Add skewed + hotspot columns if they don't exist yet
ALTER TABLE employees_cluster_small
  ADD COLUMN range_skew INT,
  ADD COLUMN hot_col    INT;

-- range_skew: many rows per value (0..100)
UPDATE employees_cluster_small
SET range_skew = FLOOR(ssnum / 1000);

-- hot_col: ~10% rows have value 1 (hotspot)
UPDATE employees_cluster_small
SET hot_col = IF(ssnum % 10 = 0, 1, ssnum);

SELECT MIN(range_skew), MAX(range_skew),
       MIN(hot_col),    MAX(hot_col)
FROM employees_cluster_small;

SET profiling = 1;

-- ================================
-- S3: RANGE on SKEWED distribution (range_skew)
--     With index vs No index
-- ================================

-- With index
CREATE INDEX idx_range_skew
ON employees_cluster_small (range_skew);

SELECT COUNT(*) FROM employees_cluster_small
WHERE range_skew BETWEEN 10 AND 20;

-- No index
DROP INDEX idx_range_skew ON employees_cluster_small;

SELECT COUNT(*) FROM employees_cluster_small
WHERE range_skew BETWEEN 10 AND 20;

-- ================================
-- S4: MULTIPOINT HOTSPOT query (hot_col)
--     With index vs No index
-- ================================

-- With index
CREATE INDEX idx_hot_col
ON employees_cluster_small (hot_col);

SELECT COUNT(*) FROM employees_cluster_small
WHERE hot_col = 1;

-- No index
DROP INDEX idx_hot_col ON employees_cluster_small;

SELECT COUNT(*) FROM employees_cluster_small
WHERE hot_col = 1;

-- Show all timings
SHOW PROFILES;
