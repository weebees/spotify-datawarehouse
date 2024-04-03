SELECT pt.dim_playlist_key,
	   AVG(dl.profanity) AS weekly_profanity_score
FROM fct_profanity_insights1 fpi
		 JOIN dim_day d ON fpi.dim_day_key = d.dim_day_key
		 JOIN dim_playlist pt ON fpi.dim_playlist_key = pt.dim_playlist_key
		 JOIN dim_track dt ON fpi.dim_track_key = dt.dim_track_key
		 JOIN dim_lyrics dl ON dl.track_id = dt.track_id
GROUP BY pt.dim_playlist_key;


SELECT
    dt.dim_time_key,
    du.dim_user_key,
    dp.dim_playlist_key,
    COUNT(fua.fct_user_activity_key) AS total_activities
FROM
    fct_user_activity1 fua
    JOIN dim_time dt ON fua.dim_time_key = dt.dim_time_key
    JOIN dim_user du ON fua.dim_user_key = du.dim_user_key
    JOIN dim_playlist dp ON fua.dim_playlist_key = dp.dim_playlist_key
WHERE
    DATE_TRUNC('minute', dt.date) = '2023-12-08'  -- replace 'specific_date' with the desired date
GROUP BY
    dt.dim_time_key, du.dim_user_key, dp.dim_playlist_key;


SELECT
    dp.dim_playlist_key,
    AVG(fua.play_count) AS avg_play_count
FROM
    fct_user_activity1 fua
    JOIN dim_playlist dp ON fua.dim_playlist_key = dp.dim_playlist_key
GROUP BY
    dp.dim_playlist_key
ORDER BY
    avg_play_count DESC;


SELECT
    dt.dim_time_key as hours,
    AVG(fua.play_count) AS avg_play_count
FROM
    fct_user_activity1 fua
    JOIN dim_time dt ON fua.dim_time_key = dt.dim_time_key
WHERE
    dt.date >= CURRENT_DATE - INTERVAL '1 month'
GROUP BY
    dt.dim_time_key
ORDER BY
    dt.dim_time_key;


WITH ranked_playlists AS (
    SELECT
        dp.playlist_id,
        dp.playlist_name,
        dp.playlist_description,
        fi.weekly_profanity_score,
        RANK() OVER (ORDER BY fi.weekly_profanity_score DESC) AS profanity_rank
    FROM
        fct_profanity_insights1 fi
        JOIN dim_playlist dp ON fi.dim_playlist_key = dp.dim_playlist_key
        JOIN dim_day dd ON fi.dim_day_key = dd.dim_day_key
    WHERE
        dd.date >= CURRENT_DATE - INTERVAL '7 days' -- Adjust the time window as needed
)

SELECT
    rp.playlist_id,
    rp.playlist_name,
    rp.playlist_description,
    rp.weekly_profanity_score,
    da.play_count,
    du.user_id,
    du.user_name,
    du.user_followers
FROM
    ranked_playlists rp
    JOIN fct_user_activity1 da ON rp.playlist_id = da.dim_playlist_key
    JOIN dim_user du ON da.dim_user_key = du.dim_user_key
WHERE
    rp.profanity_rank <= 10 -- Adjust the number of top playlists to retrieve

ORDER BY
    rp.profanity_rank;


WITH time_of_day_activity AS (
    SELECT
        fua.dim_time_key,
        dp.playlist_name,
        EXTRACT(HOUR FROM dt.date) AS hour_of_day,
        AVG(fua.play_count) AS avg_play_count
    FROM
        fct_user_activity1 fua
        JOIN dim_playlist dp ON fua.dim_playlist_key = dp.dim_playlist_key
        JOIN dim_time dt ON fua.dim_time_key = dt.dim_time_key
    GROUP BY
        fua.dim_time_key,
        dp.playlist_name,
        hour_of_day
)

SELECT
    tda.dim_time_key,
    tda.playlist_name,
    tda.hour_of_day,
    tda.avg_play_count,
    RANK() OVER (PARTITION BY tda.dim_time_key ORDER BY tda.avg_play_count DESC) AS rank_within_hour
FROM
    time_of_day_activity tda
ORDER BY
    tda.dim_time_key,
    rank_within_hour
LIMIT 20;



WITH time_of_day_activity AS (
    SELECT
        fua.dim_time_key,
        dp.playlist_name,
--         EXTRACT(HOUR FROM dt.date) AS hour_of_day,
        dt.hour AS hour_of_day,
        AVG(fua.play_count) AS avg_play_count
    FROM
        fct_user_activity1 fua
        JOIN dim_playlist dp ON fua.dim_playlist_key = dp.dim_playlist_key
        JOIN dim_time dt ON fua.dim_time_key = dt.dim_time_key
    GROUP BY
        fua.dim_time_key,
        dp.playlist_name,
        hour_of_day
)

SELECT
    tda.dim_time_key,
    tda.playlist_name,
    tda.hour_of_day,
    tda.avg_play_count,
    SUM(tda.avg_play_count) OVER (PARTITION BY tda.playlist_name ORDER BY tda.hour_of_day) AS cumulative_avg_play_count,
    RANK() OVER (PARTITION BY tda.dim_time_key ORDER BY tda.avg_play_count DESC) AS rank_within_hour
FROM
    time_of_day_activity tda
ORDER BY
    tda.dim_time_key,
    rank_within_hour;


WITH time_of_day_activity AS (
    SELECT
        fua.dim_time_key,
        dp.playlist_name,
        EXTRACT(HOUR FROM dt.date) AS hour_of_day,
        AVG(fua.play_count) AS avg_play_count,
        du.user_id,
        du.user_name
    FROM
        fct_user_activity1 fua
        JOIN dim_playlist dp ON fua.dim_playlist_key = dp.dim_playlist_key
        JOIN dim_time dt ON fua.dim_time_key = dt.dim_time_key
        JOIN dim_user du ON fua.dim_user_key = du.dim_user_key
    GROUP BY
        fua.dim_time_key,
        dp.playlist_name,
        hour_of_day,
        du.user_id,
        du.user_name
)

SELECT
    tda.dim_time_key,
    tda.playlist_name,
    tda.hour_of_day,
    tda.avg_play_count,
    tda.user_id,
    tda.user_name,
    SUM(tda.avg_play_count) OVER (PARTITION BY tda.playlist_name ORDER BY tda.hour_of_day) AS cumulative_avg_play_count,
    RANK() OVER (PARTITION BY tda.dim_time_key ORDER BY tda.avg_play_count DESC) AS rank_within_hour
FROM
    time_of_day_activity tda
ORDER BY
    tda.dim_time_key,
    rank_within_hour;


WITH user_playlist_playtime AS (
    SELECT
        fua.dim_user_key,
        fua.dim_playlist_key,
        AVG(fua.play_count) AS avg_play_duration
    FROM
        fct_user_activity1 fua
    GROUP BY
        fua.dim_user_key,
        fua.dim_playlist_key
)

SELECT
    uppt.dim_user_key,
    du.user_id,
    du.user_name,
    uppt.dim_playlist_key,
    dp.playlist_name,
    uppt.avg_play_duration
FROM
    user_playlist_playtime uppt
    JOIN dim_user du ON uppt.dim_user_key = du.dim_user_key
    JOIN dim_playlist dp ON uppt.dim_playlist_key = dp.dim_playlist_key;


WITH user_playlist_playtime AS (
    SELECT
        fua.dim_user_key,
        fua.dim_playlist_key,
        AVG(fua.play_count) AS avg_play_duration
    FROM
        fct_user_activity1 fua
    GROUP BY
        fua.dim_user_key,
        fua.dim_playlist_key
)

SELECT
    du.user_name,
    dp.playlist_name,
    SUM(uppt.avg_play_duration) AS total_play_duration
FROM
    user_playlist_playtime uppt
    JOIN dim_user du ON uppt.dim_user_key = du.dim_user_key
    JOIN dim_playlist dp ON uppt.dim_playlist_key = dp.dim_playlist_key
GROUP BY
    ROLLUP (du.user_name, dp.playlist_name);


-- Using ROLLUP we will find out the average play count per user for each playlist.
-- In simple words, the average number of plays per playlist across all users.
WITH user_playlist_playcount AS (
    SELECT
        fua.dim_user_key,
        fua.dim_playlist_key,
        AVG(fua.play_count) AS avg_play_count
    FROM
        fct_user_activity1 fua
    GROUP BY
        fua.dim_user_key,
        fua.dim_playlist_key
)

SELECT
    du.user_id,
    du.user_name,
    dp.playlist_name,
    SUM(uppt.avg_play_count) AS total_play_count
FROM
    user_playlist_playcount uppt
    JOIN dim_user du ON uppt.dim_user_key = du.dim_user_key
    JOIN dim_playlist dp ON uppt.dim_playlist_key = dp.dim_playlist_key
GROUP BY
    ROLLUP (du.user_name, du.user_id, dp.playlist_name)
ORDER BY
	user_name;

