CREATE TABLE fct_user_activity1
(
	fct_user_activity_key SERIAL PRIMARY KEY,
	dim_time_key          INT references dim_time (dim_time_key),
	dim_playlist_key      INT references dim_playlist (dim_playlist_key),
	dim_track_key         INT references dim_track (dim_track_key),
	dim_user_key          INT references dim_user (dim_user_key),
	play_count            INT
);

-- DROP TABLE fct_user_activity1;

INSERT INTO fct_user_activity1 (dim_user_key, dim_playlist_key, dim_time_key, play_count)
SELECT
    dim_user.dim_user_key,
    dim_playlist.dim_playlist_key,
    dim_track.dim_track_key,
    COUNT(*) OVER (PARTITION BY dim_track.dim_track_key) AS total_track_occurrence
FROM
    oltp_activity as oltp
JOIN
    dim_time ON DATE_TRUNC('minute', oltp.timestamp) = dim_time.date
JOIN
    dim_user ON oltp.user_id = dim_user.user_id
JOIN
    dim_playlist ON oltp.playlist_id = dim_playlist.playlist_id
JOIN
    dim_track ON oltp.track_id = dim_track.track_id
GROUP BY
    dim_user.dim_user_key, dim_playlist.dim_playlist_key, dim_track.dim_track_key;

CREATE TABLE fct_profanity_insights1
(
	fct_profanity_insights_key SERIAL PRIMARY KEY,
	dim_day_key                INT references dim_day (dim_day_key),
	dim_playlist_key           INT references dim_playlist (dim_playlist_key),
	dim_track_key              INT references dim_track (dim_track_key),
	weekly_profanity_score     DECIMAL
);

-- DROP TABLE fct_profanity_insights;
INSERT INTO fct_profanity_insights1 (dim_day_key, dim_playlist_key, dim_track_key, weekly_profanity_score)
SELECT
    d.dim_day_key,
    pt.dim_playlist_key,
    dt.dim_track_key,
    AVG(dl.profanity) AS weekly_profanity_score
FROM
    playlist_track p
JOIN
    dim_playlist pt ON p.playlist_id = pt.playlist_id
JOIN
    dim_day d ON DATE_TRUNC('week', pt.playlist_update_on) = d.date
JOIN
    dim_track dt ON p.track_id = dt.track_id
JOIN
    dim_lyrics dl ON dt.track_id = dl.track_id
GROUP BY
    d.dim_day_key, pt.dim_playlist_key, dt.dim_track_key;

SELECT * FROM fct_profanity_insights1;


SELECT COUNT(*) FROM (SELECT
    dim_user.dim_user_key,
    dim_playlist.dim_playlist_key,
    dim_track.dim_track_key,
    COUNT(*) OVER (PARTITION BY dim_track.dim_track_key) AS total_track_occurrence
FROM
    oltp_activity as oltp
JOIN
    dim_time ON DATE_TRUNC('minute', oltp.timestamp) = dim_time.date
JOIN
    dim_user ON oltp.user_id = dim_user.user_id
JOIN
    dim_playlist ON oltp.playlist_id = dim_playlist.playlist_id
JOIN
    dim_track ON oltp.track_id = dim_track.track_id
GROUP BY
    dim_user.dim_user_key, dim_playlist.dim_playlist_key, dim_track.dim_track_key)as o;
