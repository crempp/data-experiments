from bs4 import BeautifulSoup
import hashlib
import logging
import pandas as pd
import requests
import re
import time
from airflow import Dataset
from airflow.decorators import task
from gamedata.helpers import get_s3_file, put_s3_file, s3_path_exists

DELAY = 2  # seconds

HEADERS = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
BASE_METACRITIC_URL = 'https://www.metacritic.com/game'

PATH_WIKIPEDIA_RESULT = f'wikipedia_result.csv'
PATH_METACRITIC_RESULT = f'metacritic_result.csv'
DS_METACRITIC_RESULT = Dataset(f'lorenz://datalake/{PATH_METACRITIC_RESULT}')

logger = logging.getLogger(__name__)
session = requests.Session()


def format_name_for_url(name):
    # Format the game name for the URL

    # Remove ignored characters
    ignored_chars = set('.#/:&\',')
    name = ''.join([c for c in name if c not in ignored_chars])

    # replace spaces with dash
    name = re.sub(r'\s+', '-', name)

    # special case " - " or " -", etc.
    name = re.sub(r'([-–]{2,})', '-', name)

    name = name.lower()
    return name

def generate_url(game_name, platform):
    formatted_game_name = format_name_for_url(game_name)
    
    if platform == 'PS4':
        platform = 'playstation-4'
    elif platform == 'PS5':
        platform = 'playstation-5'
    else:
        raise Exception(f'Unsupported platform: {platform}')
    
    # URL of the page for the specified game
    url = f'{BASE_METACRITIC_URL}/{platform}/{formatted_game_name}'
    
    return url


def get_metacritic(game_name, url):
    logger.info(f'Retrieving...')
    logger.info(f'  Name: {game_name}')
    logger.info(f'  URL: {url}')
    # Send an HTTP GET request to the URL
    response = session.get(url.rstrip(), headers=HEADERS)

    logger.info(f'Response')
    logger.info(f'  Code: {response.status_code}')
    logger.info(f'  URL: {response.url}')
    logger.info(f'DONE\n')

    return {
        'Title': game_name,
        'URL': response.url,  # If we were redirected save the correct URL
        'Response Code': response.status_code,
        '_body': response.text,
    }


def parse(data):
    logger.info('Parsing...')
    soup = BeautifulSoup(data['_body'], 'html.parser')

    data['Publisher'] = soup.select('li.summary_detail.publisher span.data a')
    data['Publisher'] = data['Publisher'][0].text.strip() if len(data['Publisher']) > 0 else None

    data['Release'] = soup.select('li.summary_detail.release_data span.data')
    data['Release'] = data['Release'][0].text.strip() if len(data['Release']) > 0 else None

    metascore_wrap = soup.find('div', class_='metascore_wrap')
    if metascore_wrap is None:
        data['Metascore'] = None
        data['Metascore Count'] = None
    else:
        data['Metascore'] = metascore_wrap.find('span', attrs={"itemprop" : "ratingValue"})
        data['Metascore'] = data['Metascore'].text.strip() if data['Metascore'] is not None else None
        data['Metascore Count'] = metascore_wrap.select('div[class=summary] a span')
        data['Metascore Count'] = data['Metascore Count'][0].text.strip() if len(data['Metascore Count']) > 0 else None

    userscore_wrap = soup.find('div', class_='userscore_wrap')
    if userscore_wrap is None:
        data['Userscore'] = None
        data['userscore Count'] = None
    else:
        data['Userscore'] = userscore_wrap.select('div.metascore_w.user.large')
        data['Userscore'] = data['Userscore'][0].text if len(data['Userscore']) > 0 else None
        data['Userscore Count'] = userscore_wrap.select('div[class=summary] p span[class=count] a')
        data['Userscore Count'] = data['Userscore Count'][0].text.strip() if len(data['Userscore Count']) > 0 else None

    data['Summary'] = soup.find('div', class_='product_details')
    data['Summary'] = data['Summary'].select('ul li:nth-of-type(1) span[class=data] span:nth-of-type(1)')
    data['Summary'] = data['Summary'][0].text.strip() if len(data['Summary']) > 0 else None

    details_wrap = soup.select('div.details.side_details ul[class=summary_details]')[0]
    data['Developer'] = details_wrap.select('li.developer span.data a')
    data['Developer'] = data['Developer'][0].text.strip() if len(data['Developer']) > 0 else None
    data['Genres'] = details_wrap.select('li.product_genre span.data')
    data['Genres'] = list(map(lambda g: g.text.strip(), data['Genres']))
    data['Players'] = details_wrap.select('li.product_players span.data')
    data['Players'] = data['Players'][0].text.strip() if len(data['Players']) > 0 else None
    data['Rating'] = details_wrap.select('li.product_rating span.data')
    data['Rating'] = data['Rating'][0].text.strip() if len(data['Rating']) > 0 else None

    data['Image'] = soup.select('div.product_image img')
    data['Image'] = data['Image'][0]['src'] if len(data['Image']) > 0 else None

    return data


@task(
    task_id="metacritic_scrape",
    outlets=[DS_METACRITIC_RESULT]
)
def metacritic_scrape():
    first_run = False
    df_wikipedia = get_s3_file(PATH_WIKIPEDIA_RESULT)
    if (s3_path_exists(PATH_METACRITIC_RESULT)):
        df_prev_metacritic = get_s3_file(PATH_METACRITIC_RESULT, lineterminator='\n')
    else:
        df_prev_metacritic = None
        first_run = True
    data_list = []
    
    for index, row in df_wikipedia.iterrows():
        game_name = row['Title']
        platform = row['Platform']
        
        # See if we've already requested this game.
        if first_run:
            url = generate_url(game_name, platform)
        else:
            prev = df_prev_metacritic.loc[
                (df_prev_metacritic['Title'] == game_name) &
                (df_prev_metacritic['Response Code'] == 200)
            ]
            if len(prev) > 0:
                url = prev['URL'].iloc[0]
            else:
                url = generate_url(game_name, platform)
        
        data = get_metacritic(game_name, url)
        data['Platform'] = platform
        data['Wikipedia Hash'] = row['Hash']
        
        if data['Response Code'] == 200:
            data = parse(data)
            # Create a hash to uniquely identify the entry
            data['Hash'] = f"{data['Title']}{data['Developer']}{data['Publisher']}{data['Platform']}"
            data['Hash'] = re.sub(r'\W+', '', data['Hash']).lower()
            data['Hash'] = hashlib.sha256(data['Hash'].encode('utf-8')).hexdigest()
        else:
            data['Hash'] = None
        
        data.pop('_body')
        
        data_list.append(data)
    
        time.sleep(DELAY)
    
    metacritic_df = pd.DataFrame(data_list)
    put_s3_file(metacritic_df, PATH_METACRITIC_RESULT)
