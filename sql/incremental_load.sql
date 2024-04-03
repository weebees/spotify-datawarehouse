
-- '5R9FP49XjOZl8bJkYbygAw', '6xi9wzPL0itTBB4yD0rxaH'

	-- '114X8BYxajLO6VX3EypvAk'

INSERT INTO dim_playlist (playlist_id, playlist_name, playlist_description, playlist_update_on, playlist_expiry_on,
						  playlist_is_active, last_updated)
SELECT
    src.playlist_id,
    src.playlist_name,
    src.playlist_description,
    src.playlist_updated_on,
    DATE('9999-12-12'),  -- Assuming '9999-12-12' represents no expiry for practical purposes
    TRUE,  -- Assuming all playlists are active by default
    NOW()
FROM
    playlist src
WHERE

    src.playlist_id IN (SELECT DISTINCT playlist_id FROM oltp_activity)
    AND NOT EXISTS (
        SELECT 1
        FROM dim_playlist dest
        WHERE src.playlist_id = dest.playlist_id
          AND src.playlist_description = dest.playlist_description
          AND src.playlist_updated_on <= dest.last_updated
    );

-- Expire old records in dim_playlist based on last_updated timestamp
UPDATE dim_playlist AS dest
SET
    playlist_expiry_on = NOW(),
    playlist_is_active = FALSE
FROM (
    SELECT
        playlist_id,
        MAX(last_updated) AS max_last_updated
    FROM
        dim_playlist
    WHERE
        playlist_id IN (SELECT DISTINCT playlist_id FROM oltp_activity)
    GROUP BY
        playlist_id
    HAVING
        COUNT(*) > 1
) AS src
WHERE
    dest.playlist_id = src.playlist_id
    AND dest.last_updated < src.max_last_updated
    AND dest.playlist_is_active = TRUE;



SELECT * FROM dim_playlist
WHERE playlist_id = '114X8BYxajLO6VX3EypvAk';


SELECT COUNT(*)
FROM (SELECT
    src.playlist_id,
    src.playlist_name,
    src.playlist_description,
    src.playlist_updated_on,
    DATE('9999-12-12'),  -- Assuming '9999-12-12' represents no expiry for practical purposes
    TRUE,  -- Assuming all playlists are active by default
    NOW()
FROM
    playlist src
WHERE

    src.playlist_id IN (SELECT DISTINCT playlist_id FROM oltp_activity)
    AND NOT EXISTS (
        SELECT 1
        FROM dim_playlist dest
        WHERE src.playlist_id = dest.playlist_id
          AND src.playlist_description = dest.playlist_description
          AND src.playlist_updated_on <= dest.last_updated
    )
) e;


