Create or refresh streaming table Movie_ratings_gold_st
TBLPROPERTIES(delta.enableRowTracking = true,"skipChangeCommits" = true);
CREATE FLOW movie_ratings_gold_ingest_flow AS
INSERT INTO Movie_ratings_gold_st BY NAME
select MovieID as MovieID,
count(Rating) as Rating_count,
round(avg(Rating),2) as Rating_avg,
Case when round(avg(Rating),2) > 2 AND round(avg(Rating),2) <=3 THEN 'AVERAGE'  
when round(avg(Rating),2) > 3 AND round(avg(Rating),2) <=4 THEN 'GOOD'  
when round(avg(Rating),2) > 4 THEN 'EXCELLENT'
ELSE 'POOR' END Rating_status,
Review_ym as Review_ym
from stream (movie_ratings_silver_st)
group by MovieID,Review_ym
having Rating_count > 5;

/*Create or refresh live table movie_rating_history;
CREATE FLOW movie_rating_history_flow
AS AUTO CDC INTO
  movie_rating_history
FROM
  stream(Movie_ratings_gold_st)
KEYS
  (MovieID)
SEQUENCE BY
  Review_ym
COLUMNS * EXCEPT
  (Rating_count,Rating_avg, Review_ym)
STORED AS
  SCD TYPE 2;

 CREATE OR REFRESH STREAMING LIVE TABLE movie_rating_history_dlt 
TBLPROPERTIES ("quality" = "gold","skipChangeCommits" = true)
COMMENT "Clean, merged customers";
 APPLY CHANGES INTO LIVE.movie_rating_history_dlt
FROM stream(LIVE.Movie_ratings_gold_st)
  KEYS (MovieID)
  SEQUENCE BY Review_ym 
  COLUMNS * EXCEPT (Rating_count, Review_ym,Rating_avg)
  STORED AS SCD TYPE 2;
 */

 CREATE OR REFRESH STREAMING  TABLE movie_rating_history_dlt 
TBLPROPERTIES (delta.enableRowTracking = true,"skipChangeCommits" = true)
COMMENT "Clean, merged customers";
 APPLY CHANGES INTO movie_rating_history_dlt
FROM stream(Movie_ratings_gold_st)
  KEYS (MovieID)
  SEQUENCE BY Review_ym 
  COLUMNS * EXCEPT (Rating_count, Review_ym,Rating_avg)
  STORED AS SCD TYPE 2;
  