import pandas as pd


bse_code_investor = 'EUBG'
html_str =f"https://www.investor.bg/companies/{bse_code_investor}/view/"


offers_buy = pd.read_html(html_str,  encoding="utf-8")[3]
offers_sell = pd.read_html(html_str,  encoding="utf-8")[4]

print(offers_buy)
print(offers_sell)