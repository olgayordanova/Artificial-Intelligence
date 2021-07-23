CREATE VIEW free_float_market_capitalisation_view AS
	SELECT emission_bse_code_new, pcs_date, free_float_shares_count, pcs_price, free_float_shares_count*pcs_price AS total  FROM free_float as f
	JOIN emission AS e
	ON e.emission_isin = f.free_float_isin
	JOIN prices_count_shares AS p
	ON e.emission_bse_code_new = p.pcs_bse_code_new
	;
	
DROP VIEW free_float_market_capitalisation;
SELECT * FROM free_float_market_capitalisation_view;


CREATE VIEW count_deals_view AS
	SELECT  emission_bse_code_new, count(deal_bse_code_new) FROM count_deals as cd
	right join emission as e
	on e.emission_bse_code_new = cd.deal_bse_code_new
	GROUP BY emission_bse_code_new;
	
select * from count_deals_view;

CREATE VIEW weekly_volume_view AS
	SELECT pcs_bse_code_new, sum(pcs_product_price*pcs_daily_count) as weekly_volume, pcs_week
	FROM prices_count_shares
	group by pcs_bse_code_new, pcs_week;

CREATE VIEW spread_view AS
	SELECT emission_bse_code_new, spread_date, spread FROM emission as e
	JOIN spread AS s
	ON e.emission_bse_code_investor = s.spread_bse_code_investor;
	
select * from count_deals_view;
select * from weekly_volume_view;
select * from free_float_market_capitalisation_view;
select * from spread_view;

