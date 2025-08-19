CREATE OR REFRESH MATERIALIZED VIEW Top_movies_per_months_mview AS
with Movie_ratings_gold as
(select mr.MovieID as MovieID,
md.Title AS Title,
count(mr.Rating) as Rating_count,
round(avg(mr.Rating),2) as Rating_avg,
mr.Review_ym as Review_ym
from movie_ratings_silver_st mr,movie_dim md
where mr.MovieID=md.MovieID
group by mr.MovieID,mr.Review_ym,md.Title
having Rating_count > 20
order by Rating_count desc)
SELECT
    a.MovieID,
    a.Title,
    a.Rating_avg,
    a.Review_ym
FROM Movie_ratings_gold a,
(select max(Rating_avg) mx_avg,review_ym from Movie_ratings_gold
GROUP BY review_ym) b 
where a.Rating_avg=b.mx_avg
and a.review_ym=b.review_ym
order by a.review_ym 