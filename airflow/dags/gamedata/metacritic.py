from bs4 import BeautifulSoup
import os
import pandas as pd
import pathlib
import requests
import re
import time
from airflow import Dataset
from airflow.decorators import task

AIRFLOW_PATH = os.path.normpath(str(pathlib.Path(__file__).parent.resolve()) + '../../../')
METACRITIC_RESULT = Dataset(f'file:/{AIRFLOW_PATH}/datastore/metacritic.pkl')

HEADERS = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
BASE_METACRITIC_URL = 'https://www.metacritic.com/game'

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
    print(f'Retrieving...')
    print(f'  Name: {game_name}')
    print(f'  URL: {url}')
    # Send an HTTP GET request to the URL
    response = session.get(url.rstrip(), headers=HEADERS)

    print(f'Response')
    print(f'  Code: {response.status_code}')
    print(f'  URL: {response.url}')
    print(f'DONE\n')

    return {
        'title': game_name,
        'url': response.url,  # If we were redirected save the correct URL
        'response_code': response.status_code,
        'body': response.text,
    }


def parse(data):
    print('Parsing...')
    soup = BeautifulSoup(data['body'], 'html.parser')

    data['publisher'] = soup.select('li.summary_detail.publisher span.data a')
    data['publisher'] = data['publisher'][0].text.strip() if len(data['publisher']) > 0 else None

    data['release'] = soup.select('li.summary_detail.release_data span.data')
    data['release'] = data['release'][0].text.strip() if len(data['release']) > 0 else None

    metascore_wrap = soup.find('div', class_='metascore_wrap')
    if metascore_wrap is None:
        data['metascore'] = None
        data['metascore_count'] = None
    else:
        data['metascore'] = metascore_wrap.find('span', attrs={"itemprop" : "ratingValue"})
        data['metascore'] = data['metascore'].text.strip() if data['metascore'] is not None else None
        data['metascore_count'] = metascore_wrap.select('div[class=summary] a span')
        data['metascore_count'] = data['metascore_count'][0].text.strip() if len(data['metascore_count']) > 0 else None

    userscore_wrap = soup.find('div', class_='userscore_wrap')
    if userscore_wrap is None:
        data['userscore'] = None
        data['userscore_count'] = None
    else:
        data['userscore'] = userscore_wrap.select('div.metascore_w.user.large')
        data['userscore'] = data['userscore'][0].text if len(data['userscore']) > 0 else None
        data['userscore_count'] = userscore_wrap.select('div[class=summary] p span[class=count] a')
        data['userscore_count'] = data['userscore_count'][0].text.strip() if len(data['userscore_count']) > 0 else None

    data['summary'] = soup.find('div', class_='product_details')
    data['summary'] = data['summary'].select('ul li:nth-of-type(1) span[class=data] span:nth-of-type(1)')
    data['summary'] = data['summary'][0].text.strip() if len(data['summary']) > 0 else None

    details_wrap = soup.select('div.details.side_details ul[class=summary_details]')[0]
    data['developer'] = details_wrap.select('li.developer span.data a')
    data['developer'] = data['developer'][0].text.strip() if len(data['developer']) > 0 else None
    data['genres'] = details_wrap.select('li.product_genre span.data')
    data['genres'] = list(map(lambda g: g.text.strip(), data['genres']))
    data['players'] = details_wrap.select('li.product_players span.data')
    data['players'] = data['players'][0].text.strip() if len(data['players']) > 0 else None
    data['rating'] = details_wrap.select('li.product_rating span.data')
    data['rating'] = data['rating'][0].text.strip() if len(data['rating']) > 0 else None

    data['image'] = soup.select('div.product_image img')
    data['image'] = data['image'][0]['src'] if len(data['image']) > 0 else None

    print(f'  {data["title"]}')
    print(f'  {data["url"]}')
    print(f'  Metascore: {data["metascore"]} | {data["metascore_count"]} reviews')
    print(f'  Userscore: {data["userscore"]} | {data["userscore_count"]} reviews')
    print(f'  Publisher {data["publisher"]}')
    print(f'  Developer {data["developer"]}')
    print(f'  Release {data["release"]}')
    print(f'  Players: {data["players"]}')
    print(f'  Ratiing: {data["rating"]}')
    print(f'  genres {data["genres"]}')
    print(f'  {data["image"]}')
    print(f'  \n{data["summary"]}\n\n')

    return data


@task(
    task_id="metacritic_scrape",
    outlets=[METACRITIC_RESULT]
)
def metacritic_scrape():
    df_wikipedia = pd.read_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_result.pkl')
    df_prev_metacritic = pd.read_pickle(f'{AIRFLOW_PATH}/datastore/metacritic_result.pkl')
    data_list = []
    
    for index, row in df_wikipedia.iterrows():
        game_name = row['Title']
        platform = row['Platform']
        
        # See if we've already requested this game.
        
        prev = df_prev_metacritic.loc[
            (df_prev_metacritic['title'] == game_name) &
            (df_prev_metacritic['response_code'] == 200)
            ]
        if len(prev) > 0:
            url = prev['url'].iloc[0]
        else:
            url = generate_url(game_name, platform)
        
        data = get_metacritic(game_name, url)
    
        if data['response_code'] == 200:
            data = parse(data)
    
        data.pop('body')
        data_list.append(data)
    
        time.sleep(2)
    
    metacritic_df = pd.DataFrame(data_list)
    metacritic_df.to_pickle(f'{AIRFLOW_PATH}/datastore/metacritic_result.pkl')
