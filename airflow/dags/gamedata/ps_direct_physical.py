import json
import os
import pandas as pd
import pathlib
import requests
import time
from airflow import Dataset
from airflow.decorators import task

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

AIRFLOW_PATH = os.path.normpath(str(pathlib.Path(__file__).parent.resolve()) + '../../../')
TIMESTAMP = time.strftime("%Y%m%d-%H%M%S")
PATH_PS_DIRECT_PHYSICAL_RESULT = f'file:/{AIRFLOW_PATH}/datastore/archive/ps_direct_physical_result_{TIMESTAMP}.csv'
PATH_PS_DIRECT_PHYSICAL_RESULT_CURRENT = f'file:/{AIRFLOW_PATH}/datastore/ps_direct_physical_result_current.csv'
DS_PS_DIRECT_PHYSICAL_RESULT = Dataset(PATH_PS_DIRECT_PHYSICAL_RESULT)
DS_PS_DIRECT_PHYSICAL_RESULT_CURRENT = Dataset(PATH_PS_DIRECT_PHYSICAL_RESULT_CURRENT)

session = requests.Session()
@task(
    task_id="ps_direct_physical_scrape",
    retries=2,
    outlets=[DS_PS_DIRECT_PHYSICAL_RESULT, DS_PS_DIRECT_PHYSICAL_RESULT_CURRENT]
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
    
    df.to_csv(PATH_PS_DIRECT_PHYSICAL_RESULT, index=True)
    df.to_csv(PATH_PS_DIRECT_PHYSICAL_RESULT_CURRENT, index=True)
