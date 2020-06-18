import requests
from bs4 import BeautifulSoup
import pandas as pd


category = 'Loss_Costs_or_Rate_Filing'
state_codes = ['AL', 'AK', 'AZ', 'AR', 'CO', 'CT', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IA', 'KS', 'KY', 'LA', 'ME',
               'MD', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NM', 'NC', 'OK', 'OR', 'RI', 'SC', 'SD', 'TN', 'UT', 'VT',
               'VA', 'WV']
year_list = ['2020', '2019',]
test = 'https://www.ncci.com/onlinecirculars/navigator.aspx'


r = requests.get(url=test)
soup = BeautifulSoup(r.content, parser='html.parser')


for state in state_codes:
    for year in year_list:
        filename = f'{state}-{year}-01.pdf'
        url = f'https://www.ncci.com/manuals/circulars/State_Relations_Regulatory_Services/{category}/State/{state_codes}/{filename}'
        try:
            response = requests.get(url, stream=True)
            response.raw.decode_content = True
            with open(f'C://out//{filename}', 'wb') as f:
                f.write(response.content)
        except Exception as e:
            print(e)
