Create or refresh live table movie_table_bronze
as 
select _c0 as MovieID,
      _c1 as Rating,
      _c2 as Review_ym from csv.`/Volumes/movie_catalog/movie_schema/movie_user_ratings` ;



CREATE OR REFRESH LIVE TABLE movie_ratings_silver(
  CONSTRAINT valid_rating EXPECT(Rating  BETWEEN 1 AND 5),
  CONSTRAINT movie_id_fk FOREIGN KEY (MovieID) REFERENCES movie_dim 
)
COMMENT "Movie Rating table silver"
AS SELECT
  CAST(MovieID AS LONG) AS MovieID,
  CAST(Rating AS INT) AS Rating,
  Review_ym AS Review_ym,
  current_timestamp as Processing_time
FROM live.movie_table_bronze;

create or replace  materialized view Movie_ratings_gold 
as
select mr.MovieID as MovieID,
md.Title AS Title,
count(mr.Rating) as Rating_count,
round(avg(mr.Rating),2) as Rating_avg,
mr.Review_ym as Review_ym
from live.movie_ratings_silver mr,movie_dim md
where mr.MovieID=md.MovieID
group by mr.MovieID,mr.Review_ym,md.Title
having Rating_count > 20
order by Rating_count desc;