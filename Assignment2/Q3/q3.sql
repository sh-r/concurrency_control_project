\timing on
SET work_mem='2GB';
SET maintenance_work_mem='2GB';

------------------------------------------------------------
-- 0. Drop old tables (safe to re-run the script)
------------------------------------------------------------
DROP TABLE IF EXISTS likes;
DROP TABLE IF EXISTS dislikes;
DROP TABLE IF EXISTS friends;

------------------------------------------------------------
-- 1. Create base tables
------------------------------------------------------------
CREATE TABLE likes (
    person  INTEGER NOT NULL,
    artist  INTEGER NOT NULL
);

CREATE TABLE dislikes (
    person  INTEGER NOT NULL,
    artist  INTEGER NOT NULL
);

CREATE TABLE friends (
    person1 INTEGER NOT NULL,
    person2 INTEGER NOT NULL
);

------------------------------------------------------------
-- 2. Load the data
-- Must be all on ONE line per \copy command
------------------------------------------------------------

\copy likes(person, artist) FROM '/home/sr7463/Q3/like.txt'    DELIMITER ',' CSV HEADER;
\copy dislikes(person, artist) FROM '/home/sr7463/Q3/dislike.txt' DELIMITER ',' CSV HEADER;
\copy friends(person1, person2) FROM '/home/sr7463/Q3/friends.txt' DELIMITER ',' CSV HEADER;

------------------------------------------------------------
-- 3. Create essential indexes (counted in runtime)
-- These are the only indexes needed for < 2 min total time.
------------------------------------------------------------

-- Likes/dislikes indexes
CREATE INDEX idx_likes_person_artist    ON likes(person, artist);
CREATE INDEX idx_dislikes_person_artist ON dislikes(person, artist);

-- Friends indexes (two directions)
CREATE INDEX idx_friends_p1 ON friends(person1);
CREATE INDEX idx_friends_p2 ON friends(person2);

------------------------------------------------------------
-- 4. Normalize friendships (make them symmetric)
------------------------------------------------------------
DROP TABLE IF EXISTS all_friends;

CREATE TEMP TABLE all_friends AS
SELECT person1 AS u1, person2 AS u2 FROM friends
UNION
SELECT person2 AS u1, person1 AS u2 FROM friends;

CREATE INDEX idx_all_friends_u1 ON all_friends(u1);
CREATE INDEX idx_all_friends_u2 ON all_friends(u2);

------------------------------------------------------------
-- 5. myfriendlikes(u1, u2, artist)
------------------------------------------------------------
DROP TABLE IF EXISTS myfriendlikes;

CREATE TABLE myfriendlikes AS
SELECT DISTINCT
    af.u1 AS u1,
    af.u2 AS u2,
    l.artist AS artist
FROM all_friends af
JOIN likes l
    ON l.person = af.u2
LEFT JOIN likes self_like
    ON self_like.person = af.u1 AND self_like.artist = l.artist
LEFT JOIN dislikes self_dislike
    ON self_dislike.person = af.u1 AND self_dislike.artist = l.artist
WHERE self_like.person IS NULL
  AND self_dislike.person IS NULL;

-- CREATE INDEX idx_myfriendlikes_u1_artist ON myfriendlikes(u1, artist);

------------------------------------------------------------
-- 6. myfrienddislikes(u1, u2, artist)
------------------------------------------------------------
DROP TABLE IF EXISTS myfrienddislikes;

CREATE TABLE myfrienddislikes AS
SELECT DISTINCT
    af.u1 AS u1,
    af.u2 AS u2,
    d.artist AS artist
FROM all_friends af
JOIN dislikes d
    ON d.person = af.u2
LEFT JOIN likes self_like
    ON self_like.person = af.u1 AND self_like.artist = d.artist
LEFT JOIN dislikes self_dislike
    ON self_dislike.person = af.u1 AND self_dislike.artist = d.artist
WHERE self_like.person IS NULL
  AND self_dislike.person IS NULL;

-- CREATE INDEX idx_myfrienddislikes_u1_artist ON myfrienddislikes(u1, artist);

------------------------------------------------------------
-- 7. Aggregated friend-like/dislike sets
------------------------------------------------------------
DROP TABLE IF EXISTS friends_like_artist;
DROP TABLE IF EXISTS friends_dislike_artist;

CREATE TEMP TABLE friends_like_artist AS
SELECT u1 AS person, artist
FROM myfriendlikes
GROUP BY u1, artist;

CREATE TEMP TABLE friends_dislike_artist AS
SELECT u1 AS person, artist
FROM myfrienddislikes
GROUP BY u1, artist;

-- CREATE INDEX idx_fla ON friends_like_artist(person, artist);
-- CREATE INDEX idx_fda ON friends_dislike_artist(person, artist);

------------------------------------------------------------
-- 8. ishouldlike(u1, artist)
------------------------------------------------------------
DROP TABLE IF EXISTS ishouldlike;

CREATE TABLE ishouldlike AS
SELECT
    fla.person AS u1,
    fla.artist
FROM friends_like_artist fla
LEFT JOIN friends_dislike_artist fda
    ON fda.person = fla.person AND fda.artist = fla.artist
WHERE fda.person IS NULL;

------------------------------------------------------------
-- 9. Sanity checks 
------------------------------------------------------------

-- SELECT 'myfriendlikes rows', COUNT(*) FROM myfriendlikes;
-- SELECT 'myfrienddislikes rows', COUNT(*) FROM myfrienddislikes;
-- SELECT 'ishouldlike rows', COUNT(*) FROM ishouldlike;

-- SELECT COUNT(*)
-- FROM ishouldlike isl
-- LEFT JOIN myfriendlikes mfl
--   ON mfl.u1 = isl.u1 AND mfl.artist = isl.artist
-- WHERE mfl.u1 IS NULL;

-- SELECT * FROM myfriendlikes LIMIT 20;
-- SELECT * FROM myfrienddislikes LIMIT 20;
-- SELECT * FROM ishouldlike LIMIT 20;

------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------
