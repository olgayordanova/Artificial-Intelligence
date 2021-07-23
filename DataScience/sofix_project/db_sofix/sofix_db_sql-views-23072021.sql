CREATE VIEW free_float_mc_view AS
SELECT e.emission_bse_code_new,
    p.pcs_date,
    p.pcs_price,
	--f.free_float_date,
    f.free_float_shares_count,
    f.emission_shares_count
   FROM free_float f
   right JOIN emission e ON e.emission_isin::text = f.free_float_isin::text
   right JOIN prices_count_shares p ON e.emission_bse_code_new::text = p.pcs_bse_code_new::text
   WHERE f.free_float_date = p.pcs_date;

--DROP VIEW free_float_mc_view;
--SELECT * FROM free_float_mc_view