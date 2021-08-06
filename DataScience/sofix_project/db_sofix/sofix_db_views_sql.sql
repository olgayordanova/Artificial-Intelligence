--  count_deals_view
SELECT e.emission_bse_code_new,
count(cd.deal_bse_code_new) AS count
FROM count_deals cd
RIGHT JOIN emission e ON e.emission_bse_code_new::text = cd.deal_bse_code_new::text
GROUP BY e.emission_bse_code_new;
  
--   free_float_mc_view  
SELECT e.emission_bse_code_new,
p.pcs_date,
p.pcs_price,
f.free_float_shares_count,
f.emission_shares_count
FROM free_float f
RIGHT JOIN emission e ON e.emission_isin::text = f.free_float_isin::text
RIGHT JOIN prices_count_shares p ON e.emission_bse_code_new::text = p.pcs_bse_code_new::text
WHERE f.free_float_date = p.pcs_date;
  
--   spread_view
SELECT e.emission_bse_code_new,
s.spread_date,
s.spread
FROM emission e
JOIN spread s ON e.emission_bse_code_investor::text = s.spread_bse_code_investor::text;

-- weekly_volume_view
SELECT prices_count_shares.pcs_bse_code_new,
sum(prices_count_shares.pcs_product_price * prices_count_shares.pcs_daily_count::numeric) AS weekly_volume,
prices_count_shares.pcs_week
FROM prices_count_shares
GROUP BY prices_count_shares.pcs_bse_code_new, prices_count_shares.pcs_week;

	 
  
  
  
  