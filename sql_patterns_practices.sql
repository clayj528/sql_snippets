-- Scripts practiced/exercised from the book "Minimum Visible SQL Patterns" by Ergest Xheblati


-- built for the StackOverflow public dataset in BigQuery
-- Stackoverflow dataset access: https://console.cloud.google.com/marketplace/product/stack-exchange/stack-overflow?project=tangential-sled-362418
-- My BigQuery link: https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=stackoverflow&page=dataset&project=tangential-sled-362418&ws=!1m9!1m4!1m3!1stangential-sled-362418!2sbquxjob_4255dafc_183565735f2!3sUS!1m3!3m2!1sbigquery-public-data!2sstackoverflow




-- Main query (built in snippets during Chapters 1 & 2, assembled in Chapter 3)

WITH post_activity AS (
  SELECT
    ph.post_id,
    ph.user_id,
    u.display_name AS user_name,
    ph.creation_date AS activity_date,
    CASE WHEN ph.post_history_type_id IN(1,2,3) THEN 'created'
         WHEN ph.post_history_type_id IN(4,5,6) THEN 'edited'
    END AS activity_type
  FROM bigquery-public-data.stackoverflow.post_history ph
  LEFT JOIN bigquery-public-data.stackoverflow.users u
          ON u.id = ph.user_id
  WHERE 1=1
    AND ph.post_history_type_id BETWEEN 1 AND 6
    AND ph.user_id > 0
    AND ph.user_id IS NOT NULL
    AND ph.creation_date >= '2021-06-01'
    AND ph.creation_date <= '2021-09-30' 
  GROUP BY 1,2,3,4,5
)
, post_types AS (
  SELECT 
    id AS post_id,
    'question' AS post_type
  FROM bigquery-public-data.stackoverflow.posts_questions
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'
  UNION ALL
  SELECT 
    id AS post_id,
    'answer' AS post_type
  FROM bigquery-public-data.stackoverflow.posts_answers
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'
)
, user_post_metrics AS (
  SELECT
    pa.user_id,
    pa.user_name,
    CAST(pa.activity_date AS DATE) AS activity_date,
    SUM( CASE WHEN activity_type = 'created'
              THEN 1 
          ELSE 0 END) AS posts_created,
    SUM( CASE WHEN activity_type = 'edited'
              THEN 1 
          ELSE 0 END) AS posts_edited,
    SUM( CASE WHEN activity_type = 'created'
               AND post_type = 'question' 
              THEN 1 
          ELSE 0 END) AS questions_created,
    SUM( CASE WHEN activity_type = 'created'
               AND post_type = 'answer' 
              THEN 1 
          ELSE 0 END) AS answers_created,
    SUM( CASE WHEN activity_type = 'edited'
               AND post_type = 'question' 
              THEN 1 
          ELSE 0 END) AS questions_edited,
    SUM( CASE WHEN activity_type = 'edited'
               AND post_type = 'answer' 
              THEN 1 
          ELSE 0 END) AS answers_edited
  FROM post_activity pa
  INNER JOIN post_types pt 
    ON pa.post_id = pt.post_id
  GROUP BY 1,2,3
)
, comments_on_user_post AS (
  SELECT
    pa.user_id,
    CAST(c.creation_date AS DATE) AS activity_date,
    COUNT(*) AS total_comments
  FROM bigquery-public-data.stackoverflow.comments c
  INNER JOIN post_activity pa
    ON pa.post_id = c.post_id
  WHERE 1=1
    AND pa.activity_type = 'created'
    AND c.creation_date >= '2021-06-01'
    AND c.creation_date <= '2021-09-30'
  GROUP BY 1,2
)
, comments_by_user AS (
  SELECT
    user_id,
    CAST(creation_date AS DATE) AS activity_date,
    COUNT(*) AS total_comments
  FROM bigquery-public-data.stackoverflow.comments
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'
  GROUP BY 1,2
)
, votes_on_user_post AS (
  SELECT
    pa.user_id,
    CAST(v.creation_date AS DATE) AS activity_date,
    SUM(CASE WHEN vote_type_id = 2 THEN 1 ELSE 0 END) AS total_upvotes,
    SUM(CASE WHEN vote_type_id = 3 THEN 1 ELSE 0 END) AS total_downvotes
  FROM bigquery-public-data.stackoverflow.votes v
  INNER JOIN post_activity pa
    ON pa.post_id = v.post_id
  WHERE 1=1
    AND pa.activity_type = 'created'
    AND v.creation_date >= '2021-06-01'
    AND v.creation_date <= '2021-09-30'
  GROUP BY 1,2
)
SELECT
  pm.user_id,
  pm.user_name,
  CAST(SUM(pm.posts_created) AS NUMERIC) AS total_posts_created,
  CAST(SUM(pm.posts_edited) AS NUMERIC) AS total_posts_edited,
  CAST(SUM(pm.answers_created) AS NUMERIC) AS total_answers_created,
  CAST(SUM(pm.answers_edited) AS NUMERIC) AS total_answers_edited,
  CAST(SUM(pm.questions_created) AS NUMERIC) AS total_questions_created,
  CAST(SUM(pm.questions_edited) AS NUMERIC) AS total_questions_edited,
  CAST(SUM(vu.total_upvotes) AS NUMERIC) AS total_upvotes,
  CAST(SUM(vu.total_downvotes) AS NUMERIC) AS total_downvotes,
  CAST(SUM(cu.total_comments) AS NUMERIC) AS total_comments_by_user,
  CAST(SUM(cp.total_comments) AS NUMERIC) AS total_comments_on_post,
  CAST(COUNT(DISTINCT pm.activity_date) AS NUMERIC) AS streak_in_days
FROM user_post_metrics pm
INNER JOIN votes_on_user_post vu 
  ON vu.activity_date = pm.activity_date
  AND vu.user_id = pm.user_id
INNER JOIN comments_on_user_post cp 
  ON cp.activity_date = pm.activity_date
  AND cp.user_id = pm.user_id
INNER JOIN comments_by_user cu 
  ON cu.activity_date = pm.activity_date
  AND cu.user_id = pm.user_id
GROUP BY 1,2











-- Manipulating and SUBSTRINGing data with different metrics
WITH weights AS (
  SELECT '32.5lb' AS wt
  UNION ALL
  SELECT '45.2lb' AS wt
  UNION ALL
  SELECT '53.1lb' AS wt
  UNION ALL
  SELECT '77kg' AS wt
  UNION ALL
  SELECT '68kg' AS wt
)
SELECT CAST (CASE WHEN wt LIKE '%lb'
                  THEN SUBSTRING(wt,1,INSTR(wt,'lb')-1)
                  WHEN wt LIKE '%kg'
                  THEN SUBSTRING(wt,1,INSTR(wt,'kg')-1)
                  END AS DECIMAL) AS weight,
       CASE WHEN wt LIKE '%lb' THEN 'LB'
            WHEN wt LIKE '%kg' THEN 'KG'
       END AS unit
FROM weights
;




-- Manipulating and CASTing bad data types
-- Taking out two separate formatting issues
WITH dates AS (
  SELECT '2021-12--01' AS dt
  UNION ALL
  SELECT '2021-12--02' AS dt
  UNION ALL
  SELECT '2021-12--03' AS dt
  UNION ALL
  SELECT '12/04/2021' AS dt
  UNION ALL
  SELECT '12/05/2021' AS dt
  UNION ALL
  SELECT '13/05/2021' AS dt
)
SELECT 
  COALESCE(
       SAFE_CAST(CASE WHEN dt LIKE '%-%--%'
                      THEN SUBSTRING(dt,1,4) || '-' ||
                           SUBSTRING(dt,6,2) || '-' ||
                           SUBSTRING(dt,10,2)
                      WHEN dt LIKE '%/%/%'
                      THEN SUBSTRING(dt,7,4) || '-' ||
                           SUBSTRING(dt,1,2) || '-' ||
                          SUBSTRING(dt,4,2) 
                      ELSE NULL
                      END AS DATE), '1987-05-28') AS date_field
FROM dates


-- Single issue correction
WITH dates AS (
  SELECT '2021-12--01' AS dt
  UNION ALL
  SELECT '2021-12--02' AS dt
  UNION ALL
  SELECT '2021-12--03' AS dt
  UNION ALL
  SELECT '2021-12--04' AS dt
  UNION ALL
  SELECT '2021-12--05' AS dt
)
SELECT CAST (SUBSTRING(dt,1,4) || '-' ||
             SUBSTRING(dt,6,2) || '-' ||
             SUBSTRING(dt,10,2) AS DATE) AS date_field
FROM dates







-- Identifying tags in strings 
SELECT 
  q.id AS post_id,
  CAST(DATE_TRUNC(q.creation_date, DAY) AS DATE) AS activity_date,
  q.tags
FROM bigquery-public-data.stackoverflow.posts_questions q
WHERE 1=1
  -- AND creation_date >= '2021-06-01'
  -- AND creation_date <= '2021-09-30'
  AND CAST(DATE_TRUNC(q.creation_date, DAY) AS DATE) >= DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY)
  AND INSTR(tags, "|sql|") > 0 -- 1st approach: INSTR()
  AND tags LIKE "%|sql|%"      -- 2nd approach: LIKE
LIMIT 10







-- Creating a table as a reference, instead of using a cte
CREATE OR REPLACE VIEW v_post_types AS 

  SELECT
    id AS post_id,
    'question' AS post_type
  FROM bigquery-public-data.stackoverflow.posts_questions
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'

UNION ALL

  SELECT
    id AS post_id,
    'answer' AS post_type
  FROM bigquery-public-data.stackoverflow.posts_answers
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'












-- Unpivoted Comments activity from post_activity
WITH post_activity AS (
  SELECT
    ph.post_id,
    ph.user_id,
    u.display_name AS user_name,
    ph.creation_date AS activity_date,
    CASE WHEN ph.post_history_type_id IN(1,2,3) THEN 'created'
         WHEN ph.post_history_type_id IN(4,5,6) THEN 'edited'
    END AS activity_type
  FROM bigquery-public-data.stackoverflow.post_history ph
  LEFT JOIN bigquery-public-data.stackoverflow.users u
          ON u.id = ph.user_id
  WHERE 1=1
    AND ph.post_history_type_id BETWEEN 1 AND 6
    AND ph.user_id > 0
    AND ph.user_id IS NOT NULL
    AND ph.creation_date >= '2021-06-01'
    AND ph.creation_date <= '2021-09-30' 
  GROUP BY 1,2,3,4,5
)
, post_types AS (
  SELECT 
    id AS post_id,
    'question' AS post_type
  FROM bigquery-public-data.stackoverflow.posts_questions
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'

  UNION ALL

  SELECT 
    id AS post_id,
    'answer' AS post_type
  FROM bigquery-public-data.stackoverflow.posts_answers
  WHERE 1=1
    AND creation_date >= '2021-06-01'
    AND creation_date <= '2021-09-30'
)
, comments_on_user_post AS (
  SELECT
    pa.user_id,
    CAST(c.creation_date AS DATE) AS activity_date,
    COUNT(*) AS total_comments
  FROM bigquery-public-data.stackoverflow.comments c
  JOIN post_activity pa
    ON pa.post_id = c.post_id
  WHERE 1=1
    AND pa.activity_type = 'created'
    AND c.creation_date >= '2021-06-01'
    AND c.creation_date <= '2021-09-30'
  GROUP BY 1,2
)
SELECT *
FROM comments_on_user_post








-- Check for table Granularity
SELECT
  creation_date,
  post_id,
  post_history_type_id AS type_id,
  user_id,
  COUNT(*) AS total
FROM bigquery-public-data.stackoverflow.post_history
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
;



-- Grouping type_id to make granularity finer
SELECT
  ph.post_id,
  ph.user_id,
  ph.creation_date AS activity_date,
  CASE WHEN ph.post_history_type_id IN(1,2,3) THEN 'created'
       WHEN ph.post_history_type_id IN(4,5,6) THEN 'edited'
  END AS activity_type
FROM bigquery-public-data.stackoverflow.post_history ph
WHERE TRUE
  AND ph.post_history_type_id BETWEEN 1 AND 6
  AND ph.user_id > 0
  AND ph.user_id IS NOT NULL
  AND ph.creation_date >= '2021-06-01'
  AND ph.creation_date <= '2021-09-30'
  AND ph.post_id = 69301792
GROUP BY 1,2,3,4
;



-- Bring the DATETIME to only a DATE, to make granularity less fine
SELECT
  ph.post_id,
  ph.user_id,
  CAST(ph.creation_date AS DATE) AS activity_date,
  CASE WHEN ph.post_history_type_id IN(1,2,3) THEN 'created'
       WHEN ph.post_history_type_id IN(4,5,6) THEN 'edited'
  END AS activity_type,
  COUNT(*) AS total
FROM bigquery-public-data.stackoverflow.post_history ph
WHERE TRUE
  AND ph.post_history_type_id BETWEEN 1 AND 6
  AND ph.user_id > 0
  AND ph.user_id IS NOT NULL
  AND ph.creation_date >= '2021-06-01'
  AND ph.creation_date <= '2021-09-30'
  AND ph.post_id = 69301792
GROUP BY 1,2,3,4
;



-- Pivoting columns and rows for Created vs Edited
SELECT
  ph.post_id,
  ph.user_id,
  CAST(ph.creation_date AS DATE) AS activity_date,
  SUM(CASE WHEN ph.post_history_type_id IN(1,2,3) THEN 1
      ELSE 0 END) AS created,
  SUM(CASE WHEN ph.post_history_type_id IN(4,5,6) THEN 1
      ELSE 0 END) AS activity_type
FROM bigquery-public-data.stackoverflow.post_history ph
WHERE TRUE
  AND ph.post_history_type_id BETWEEN 1 AND 6
  AND ph.user_id > 0
  AND ph.user_id IS NOT NULL
  AND ph.creation_date >= '2021-06-01'
  AND ph.creation_date <= '2021-09-30'
  AND ph.post_id = 69301792
GROUP BY 1,2,3
;


-- Finding user Neutrino in users table
SELECT
  id,
  display_name,
  creation_date,
  reputation
FROM bigquery-public-data.stackoverflow.users
WHERE id = 8974849
;



-- Looking up neutrino in post_history, where she will have multiple posts
SELECT
  ph.post_id,
  ph.creation_date,
  ph.post_id,
  ph.post_history_type_id,
  user_id
FROM bigquery-public-data.stackoverflow.post_history ph
WHERE 1=1
  AND ph.creation_date >= '2021-06-01'
  AND ph.creation_date <= '2021-09-30'
  AND ph.user_id = 8974849
;

-- Joining on user_id
SELECT
  ph.post_id,
  ph.user_id,
  u.display_name AS user_name,
  ph.creation_date AS activity_date,
  ph.post_history_type_id AS type_id
FROM bigquery-public-data.stackoverflow.post_history ph
     JOIN bigquery-public-data.stackoverflow.users u
          ON u.id = ph.user_id
WHERE 1=1
  AND ph.post_id = 4
;


-- Same but with LEFT JOIN 
SELECT
  ph.post_id,
  ph.user_id,
  u.display_name AS user_name,
  ph.creation_date AS activity_date,
  ph.post_history_type_id AS type_id
FROM bigquery-public-data.stackoverflow.post_history ph
     LEFT JOIN bigquery-public-data.stackoverflow.users u
          ON u.id = ph.user_id
WHERE 1=1
  AND ph.post_id = 4
ORDER BY activity_date
;




-- Same but with LEFT JOIN : note the predicate in the JOIN instead of the WHERE clause, which avoids turning the left join into an inner join
SELECT
  ph.post_id,
  ph.user_id,
  u.display_name AS user_name,
  ph.creation_date AS activity_date
FROM bigquery-public-data.stackoverflow.post_history ph
     LEFT JOIN bigquery-public-data.stackoverflow.users u
          ON u.id = ph.user_id
          -- AND u.reputation > 50 
WHERE 1=1
  AND ph.post_id = 4
  AND u.id IS NULL
ORDER BY activity_date









