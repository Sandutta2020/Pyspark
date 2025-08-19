Create or refresh STREAMING TABLE  movie_ratings_bronze_st
COMMENT "Movie Ratings landing table";

CREATE FLOW movie_ratings_bronze_ingest_flow AS
INSERT INTO movie_ratings_bronze_st BY NAME
  SELECT *
  FROM STREAM read_files(
    "/Volumes/movie_catalog/movie_schema/movie_user_ratings",
    format => "csv",
    header => "true"
  );

