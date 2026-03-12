-- ======================================================
-- DATA PIPELINE - MOVIELENS / NETFLIX STYLE ANALYSIS
-- BigQuery SQL
-- ======================================================



-- ======================================================
-- DIMENSION TABLE
-- ======================================================

-- dim_movies

CREATE OR REPLACE TABLE `pipeline-netflix-dadosportodos.netflix_analytical.dim_movies` AS
SELECT
    SAFE_CAST(movieId AS INT64) AS movie_id,
    CAST(title AS STRING) AS title,
    CAST(genres AS STRING) AS genres,
    SAFE_CAST(
        REGEXP_EXTRACT(
            CAST(title AS STRING),
            r'\((\d{4})\)\s*$'
        ) AS INT64
    ) AS release_year
FROM `pipeline-netflix-dadosportodos.netflix_raw.raw_movies`;



-- ======================================================
-- FACT TABLE
-- ======================================================

-- fact_ratings

CREATE OR REPLACE TABLE `pipeline-netflix-dadosportodos.netflix_analytical.fact_ratings` AS

WITH all_ratings AS (

SELECT
    SAFE_CAST(NULLIF(userId,'') AS INT64) AS user_id,
    SAFE_CAST(NULLIF(movieId,'') AS INT64) AS movie_id,

    SAFE_CAST(NULLIF(NULLIF(rating,'NA'),'') AS FLOAT64) AS rating,

    COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', tstamp),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', tstamp)
    ) AS rating_ts,

    'user_rating_history' AS src

FROM `pipeline-netflix-dadosportodos.netflix_raw.raw_user_rating_history`

UNION ALL

SELECT
    SAFE_CAST(NULLIF(userId,'') AS INT64) AS user_id,
    SAFE_CAST(NULLIF(movieId,'') AS INT64) AS movie_id,

    SAFE_CAST(NULLIF(NULLIF(rating,'NA'),'') AS FLOAT64) AS rating,

    COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', tstamp),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', tstamp)
    ) AS rating_ts,

    'ratings_for_additional_users' AS src

FROM `pipeline-netflix-dadosportodos.netflix_raw.raw_ratings_for_additional_users`

)

SELECT
    user_id,
    movie_id,
    rating,
    rating_ts,
    src
FROM all_ratings
WHERE user_id IS NOT NULL
  AND movie_id IS NOT NULL
  AND rating IS NOT NULL

-- ======================================================
-- ANALYTICAL VIEWS
-- ======================================================

-- vw_user_activity

CREATE OR REPLACE VIEW `pipeline-netflix-dadosportodos.netflix_analytical.vw_user_activity` AS
SELECT
    user_id,
    COUNT(*) AS total_ratings,
    COUNT(DISTINCT movie_id) AS distinct_movies_rated,
    AVG(rating) AS avg_rating,
    STDDEV(rating) AS std_rating,
    MIN(rating_ts) AS first_activity_ts,
    MAX(rating_ts) AS last_activity_ts
FROM `pipeline-netflix-dadosportodos.netflix_analytical.fact_ratings`
GROUP BY 1
ORDER BY total_ratings DESC, avg_rating DESC;

-- vw_top_filmes_bem_avaliados

SELECT
    movie_id,
    title,
    genres,
    release_year,
    total_ratings,
    ROUND(avg_rating, 2) AS avg_rating
FROM `pipeline-netflix-dadosportodos.netflix_analytical.vw_movies_kpis`
WHERE total_ratings >= 50
  AND avg_rating BETWEEN 0 AND 5
ORDER BY avg_rating DESC, total_ratings DESC

LIMIT 10

-- vw_top_filmes_mais_avaliados
SELECT
    movie_id,
    title,
    genres,
    release_year,
    total_ratings,
    ROUND(avg_rating, 2) AS avg_rating
FROM `pipeline-netflix-dadosportodos.netflix_analytical.vw_movies_kpis`
ORDER BY total_ratings DESC
LIMIT 10

-- vw_avaliacoes_por_ano
  SELECT
    EXTRACT(YEAR FROM rating_ts) AS ano,
    COUNT(*) AS total_avaliacoes
FROM `pipeline-netflix-dadosportodos.netflix_analytical.fact_ratings`
GROUP BY ano
ORDER BY ano

-- vw_genre_performance
WITH exploded AS (
SELECT
    r.rating,
    genre
FROM `pipeline-netflix-dadosportodos.netflix_analytical.fact_ratings` r
JOIN `pipeline-netflix-dadosportodos.netflix_analytical.dim_movies` m
    ON m.movie_id = r.movie_id
CROSS JOIN UNNEST(SPLIT(COALESCE(m.genres, ''), '|')) AS genre
)

SELECT
    genre,
    COUNT(*) AS total_ratings,
    AVG(rating) AS avg_rating,
    STDDEV(rating) AS std_rating
FROM exploded
WHERE genre IS NOT NULL
  AND genre != ''
  AND genre != '(no genres listed)'
GROUP BY 1
ORDER BY total_ratings DESC, avg_rating DESC
  AND rating_ts IS NOT NULL;
