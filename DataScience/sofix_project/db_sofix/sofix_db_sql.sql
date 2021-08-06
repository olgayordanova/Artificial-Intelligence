CREATE table emission (
	emission_id INTEGER NOT NULL PRIMARY KEY,
	emission_name CHARACTER VARYING(25),
	emission_isin CHARACTER VARYING(12) UNIQUE,
	emission_bse_code_old CHARACTER VARYING(8) UNIQUE,
	emission_bse_code_new CHARACTER VARYING(8) UNIQUE,
	emission_bse_code_investor CHARACTER VARYING(8) UNIQUE
);

CREATE table prices_count_shares (
	pcs_id INTEGER NOT NULL PRIMARY KEY,
	pcs_date TIMESTAMP,
	pcs_bse_code_new CHARACTER VARYING(8),
	pcs_price DECIMAL,
	pcs_daily_count INTEGER,	
	FOREIGN KEY (pcs_bse_code_new) REFERENCES emission(emission_bse_code_new) ON DELETE CASCADE
);

CREATE table count_deals (
	deal_id INTEGER NOT NULL PRIMARY KEY,
	deal_date DATE,
	deal_time TIME,
	deal_bse_code_new CHARACTER VARYING(8),
	deal_price DECIMAL,
	deal_count INTEGER,	
	FOREIGN KEY (deal_bse_code_new) REFERENCES emission(emission_bse_code_new) ON DELETE CASCADE
);

CREATE table free_float (
	free_float_id INTEGER NOT NULL PRIMARY KEY,
	free_float_date DATE,
	free_float_isin CHARACTER VARYING(12),
	emission_shares_count INTEGER,
	free_float_shares_count INTEGER,
	shareholders_count INTEGER,
	FOREIGN KEY (free_float_isin) REFERENCES emission(emission_isin) ON DELETE CASCADE
);

CREATE table spread (
	spread_id INTEGER NOT NULL PRIMARY KEY,
	spread_bse_code_investor CHARACTER VARYING(8),
	free_float_date DATE,
	spread DOUBLE PRECISION,
	FOREIGN KEY (spread_bse_code_investor) REFERENCES emission(emission_bse_code_investor) ON DELETE CASCADE
);
