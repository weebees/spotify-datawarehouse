-- DIMS!
INSERT INTO dim_user (user_id, user_name, user_followers, user_added_on, last_updated)
SELECT user_id, user_name, user_followers, user_added_on, NOW()
FROM user_details
WHERE user_id IN (SELECT DISTINCT user_id
				  FROM oltp_activity);

INSERT INTO dim_playlist (playlist_id, playlist_name, playlist_description, playlist_update_on, playlist_expiry_on,
						  playlist_is_active, last_updated)
SELECT playlist_id, playlist_name, playlist_description, playlist_updated_on, DATE('9999-12-12'), TRUE, NOW()
FROM playlist
WHERE playlist_id IN (SELECT DISTINCT playlist_id
					  FROM oltp_activity);

INSERT INTO dim_track (track_id, track_name, track_award_nomination_2021, track_award_nomination_2022,
					   track_award_nomination_2023, track_award_wins_2021, track_award_wins_2022, track_award_wins_2023, last_updated)
SELECT track_id,
	   track_name,
	   -1,
	   -1,
	   -1,
	   -1,
	   -1,
	   -1,
	   NOW()
FROM track
WHERE track_id IN (SELECT DISTINCT track_id
				   FROM oltp_activity);

INSERT INTO dim_lyrics
SELECT track_id, track_lyrics, -1
FROM lyrics
WHERE track_id IN (SELECT DISTINCT track_id FROM oltp_activity);

-- FACTS!

