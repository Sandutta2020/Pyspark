-- Please edit the sample below

CREATE MATERIALIZED VIEW Top_movies_per_months AS
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
order by a.review_ym;
