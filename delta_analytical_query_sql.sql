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
    du.user_name,
    dp.playlist_name,
    SUM(uppt.avg_play_count) AS average_playcount
FROM
    user_playlist_playcount uppt
    JOIN dim_user du ON uppt.dim_user_key = du.dim_user_key
    JOIN dim_playlist dp ON uppt.dim_playlist_key = dp.dim_playlist_key
GROUP BY
    ROLLUP (du.user_name, dp.playlist_name)
ORDER BY
	user_name;


-- Further, using Dense rank we can determine the rank for average play count
WITH playlist_avg_playcount AS (
    SELECT
        dp.dim_playlist_key,
        dp.playlist_name,
        AVG(fua.play_count) AS avg_play_count,
        DENSE_RANK() OVER (ORDER BY AVG(fua.play_count) DESC) AS playlist_rank
    FROM
        fct_user_activity1 fua
        JOIN dim_playlist dp ON fua.dim_playlist_key = dp.dim_playlist_key
    GROUP BY
        dp.dim_playlist_key, dp.playlist_name
)

SELECT
    pap.playlist_name,
    pap.avg_play_count,
    pap.playlist_rank,
    RANK() OVER (ORDER BY pap.avg_play_count DESC) AS overall_rank
FROM
    playlist_avg_playcount pap
ORDER BY
    pap.playlist_rank;

