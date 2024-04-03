-- Dim Table DDLs
CREATE TABLE dim_day(
	dim_day_key SERIAL PRIMARY KEY,
	day INT,
	month INT,
	year INT,
	date TIMESTAMP
);

-- DROP TABLE dim_day;

CREATE TABLE dim_playlist(
	dim_playlist_key SERIAL PRIMARY KEY,
	playlist_id VARCHAR(255),
	playlist_name VARCHAR(255),
	playlist_description TEXT,

	-- To maintain SCD 2 following fields
	playlist_update_on TIMESTAMP,
	playlist_expiry_on TIMESTAMP,
	playlist_is_active BOOLEAN,
	last_updated TIMESTAMP
);

-- DROP TABLE dim_playlist;

CREATE TABLE dim_track(
	dim_track_key SERIAL PRIMARY KEY,
	track_id VARCHAR(255),
	track_name VARCHAR(255),

	-- To maintain SCD 3 following fields
	track_award_nomination_2021 INT,
	track_award_nomination_2022 INT,
	track_award_nomination_2023 INT,
	track_award_wins_2021 INT,
	track_award_wins_2022 INT,
	track_award_wins_2023 INT,
	last_updated TIMESTAMP
);

DROP TABLE dim_track;

CREATE TABLE dim_lyrics(
	track_id VARCHAR(255) PRIMARY KEY, -- references dim_track(track_id),
	lyrics TEXT,
	profanity DECIMAL
);

-- DROP TABLE dim_lyrics;

CREATE TABLE dim_time(
	dim_time_key SERIAL PRIMARY KEY,
	day INT,
	month INT,
	year INT,
	hour INT,
	minute INT,
	date TIMESTAMP
);

-- DROP TABLE dim_time;

CREATE TABLE dim_user(
	dim_user_key SERIAL PRIMARY KEY,
	user_id VARCHAR(255),
	user_name VARCHAR(255),
	user_followers INT,
	user_added_on TIMESTAMP,
	last_updated TIMESTAMP
);

-- DROP TABLE dim_user;


-- Fact Table DDLs
CREATE TABLE fct_profanity_insights(
	fct_profanity_insights_key SERIAL PRIMARY KEY,
	dim_day_key INT references dim_day(dim_day_key),
	dim_playlist_key INT references dim_playlist(dim_playlist_key),
	dim_track_key INT references dim_track(dim_track_key),
	weekly_profanity_score DECIMAL,
	last_updated TIMESTAMP
);

-- DROP TABLE fct_profanity_insights;

CREATE TABLE fct_user_activity (
	fct_user_activity_key SERIAL PRIMARY KEY,
	dim_time_key INT references dim_time(dim_time_key),
	dim_playlist_key INT references dim_playlist(dim_playlist_key),
	dim_track_key INT references dim_track(dim_track_key),
	dim_user_key INT references dim_user(dim_user_key),
	average_play_count INT,
	last_updated TIMESTAMP
);

-- DROP TABLE fct_user_activity;

CREATE TABLE oltp_activity(
	playlist_id VARCHAR(255),
	track_id VARCHAR(255),
	user_id VARCHAR(255),
	timestamp TIMESTAMP
);

CREATE TABLE source_data(
	playlist_id VARCHAR(255),
	track_id VARCHAR(255),
	user_id VARCHAR(255),
	timestamp TIMESTAMP
);