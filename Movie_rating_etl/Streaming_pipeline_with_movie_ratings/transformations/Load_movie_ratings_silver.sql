-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE
    movie_ratings_silver_st
    (CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT Valid_id EXPECT (MovieID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ratings EXPECT (Rating IN (2,3,4)) ON VIOLATION DROP ROW
)
COMMENT "Cleaning data from bronze";

CREATE FLOW movie_ratings_silver_ingest_flow AS
INSERT INTO movie_ratings_silver_st BY NAME
SELECT * FROM STREAM movie_ratings_bronze_st;