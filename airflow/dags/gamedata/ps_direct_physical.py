import json
import pandas as pd
import requests
import time
from airflow import Dataset
from airflow.decorators import task
from gamedata.helpers import get_s3_file, put_s3_file, s3_path_exists

DELAY = 1  # second

BASE_URL = 'https://api.direct.playstation.com/commercewebservices/ps-direct-us/products/category/SIECategory101?fields=FULL'
HEADERS = [
    'availableForPickup',
    'baseProduct',
    'code',
    'configurable',
    'description',
    'images',
    'isMilitaryRestricted',
    'klarnaEnabled',
    'loginGated',
    'name',
    'preOrderProduct',
    'streetDate',
    'url',
    'validProductCode',
    'volumePricesFlag',
    'price.basePrice',
    'price.currencyIso',
    'price.currencySymbol',
    'price.decimalPrice',
    'price.formattedValue',
    'price.priceType',
    'price.value',
    'stock.isProductLowStock',
    'stock.stockLevelStatus',
    'merchandisingBadge'
]

PATH_PS_DIRECT_PHYSICAL_RESULT = f'ps_direct_physical_result_current.csv'
DS_PS_DIRECT_PHYSICAL_RESULT = Dataset(f'lorenz://datalake/{PATH_PS_DIRECT_PHYSICAL_RESULT}')

session = requests.Session()
@task(
    task_id="ps_direct_physical_scrape",
    retries=2,
    outlets=[DS_PS_DIRECT_PHYSICAL_RESULT]
)
def ps_direct_physical_scrape():
    current_page = 0
    sort = 'onlineDate-desc'
    
    df = pd.DataFrame(columns=HEADERS)
    
    while True:
        url = f'{BASE_URL}&currentPage={current_page}&sort={sort}'
        print(url)
        response = session.get(url)
        
        j = json.loads(response.text)
        
        total_pages = j['pagination']['totalPages']
        total_results = j['pagination']['totalResults']
        
        products = j['products']
        df_page = pd.json_normalize(products)
        df_page['images'] = df_page['images'].map(lambda a: a[0]['url'])
        
        df = df.merge(df_page, how='outer')
        
        print(f'total_pages {total_pages}')
        print(f'current_page {current_page}')
        
        current_page = current_page + 1
        
        print(f'new_page {current_page}')
        print(f'df_page {len(df_page)}')
        print(f'df {len(df)}')
        print()
        
        if current_page >= total_pages:
            break
        
        time.sleep(DELAY)
    
    put_s3_file(df, PATH_PS_DIRECT_PHYSICAL_RESULT)
