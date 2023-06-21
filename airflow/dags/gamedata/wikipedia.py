from bs4 import BeautifulSoup
from dateutil.parser import parse, ParserError
import hashlib
import os
import pandas as pd
import pathlib
import requests
import re
from airflow import Dataset
from airflow.decorators import task, task_group

url_wikipedia_ps5 = 'https://en.wikipedia.org/wiki/List_of_PlayStation_5_games'
url_wikipedia_ps4_al = 'https://en.wikipedia.org/wiki/List_of_PlayStation_4_games_(A%E2%80%93L)'
url_wikipedia_ps4_mz = 'https://en.wikipedia.org/wiki/List_of_PlayStation_4_games_(M%E2%80%93Z)'

AIRFLOW_PATH = os.path.normpath(str(pathlib.Path(__file__).parent.resolve()) + '../../../')
WIKIPEDIA_PS4_A = Dataset(f'file:/{AIRFLOW_PATH}/datastore/wikipedia_ps4_a.pkl')
WIKIPEDIA_PS4_M = Dataset(f'file:/{AIRFLOW_PATH}/datastore/wikipedia_ps4_m.pkl')
WIKIPEDIA_PS5 = Dataset(f'file:/{AIRFLOW_PATH}/datastore/wikipedia_ps5.pkl')
WIKIPEDIA_RESULT = Dataset(f'file:/{AIRFLOW_PATH}/datastore/wikipedia_result.pkl')


def get_page(url):
    response = requests.get(url)
    return response.text


def extract_list(bs):
    # Search for multiple items within tags
    items = bs.find_all(['a', 'li', 'p'])
    
    # If we didn't find anything return the plain text
    if items is None or len(items) == 0:
        return (bs.text.strip(),)
    else:
        return tuple(i.text.strip() for i in items)


def extract_link(c):
    linked = c.find_all('a', href=True)
    if len(linked) > 0:
        return f'https://en.wikipedia.org{linked[0]["href"]}'
    else:
        return None


def parse_date(v):
    try:
        return parse(v.text.strip())
    except ParserError:
        return None

def parse_wikipedia_list(soup, platform):
    # Get the table of games
    table = soup.find('table', id='softwarelist')
    
    # Extract all the table rows
    tr = table.find_all('tr')[2:]
    
    # For each Row extract data cells
    raw_cells = list(map(lambda x: x.find_all(['th', 'td']), tr))
    
    # Create a data Frame
    df_0 = pd.DataFrame(raw_cells)
    df_0.columns = ['Title',
                    'Genre',
                    'Developer',
                    'Publisher',
                    'Release Date JP',
                    'Release Date NA',
                    'Release Date PAL',
                    'Addons',
                    'Ref']
    
    df = pd.DataFrame()
    
    # Clean Data
    df['Wikipedia Link'] = df_0['Title'].map(extract_link)
    df['Title'] = df_0['Title'].map(lambda a: a.text.strip())
    df['Genre'] = df_0['Genre'].map(extract_list).to_list()  # TODO: Normalize genres
    df['Developer'] = df_0['Developer'].map(lambda a: a.text.strip())
    df['Publisher'] = df_0['Publisher'].map(lambda a: a.text.strip())
    df['Unreleased JP'] = df_0['Release Date JP'].map(
        lambda a: a.text.strip() == 'Unreleased')
    df['Unreleased NA'] = df_0['Release Date NA'].map(
        lambda a: a.text.strip() == 'Unreleased')
    df['Unreleased PAL'] = df_0['Release Date PAL'].map(
        lambda a: a.text.strip() == 'Unreleased')
    df['TBA JP'] = df_0['Release Date JP'].map(
        lambda a: a.text.strip() == 'TBA')
    df['TBA NA'] = df_0['Release Date NA'].map(
        lambda a: a.text.strip() == 'TBA')
    df['TBA PAL'] = df_0['Release Date PAL'].map(
        lambda a: a.text.strip() == 'TBA')
    df['Release Date JP'] = df_0['Release Date JP'].map(parse_date)
    df['Release Date NA'] = df_0['Release Date NA'].map(parse_date)
    df['Release Date PAL'] = df_0['Release Date PAL'].map(parse_date)
    df['Crossbuy'] = df_0['Addons'].map(
        lambda a: False if a is None else 'CB' in a.text)
    df['Crossplay'] = df_0['Addons'].map(
        lambda a: False if a is None else 'CP' in a.text)
    df['3DTV'] = df_0['Addons'].map(
        lambda a: False if a is None else '3D' in a.text)
    df['PS Camera'] = df_0['Addons'].map(
        lambda a: False if a is None else 'C' in a.text)
    df['PS4 Pro Enhanced'] = df_0['Addons'].map(
        lambda a: False if a is None else 'P' in a.text)
    df['Play Link'] = df_0['Addons'].map(
        lambda a: False if a is None else 'PL' in a.text)
    df['PSVR'] = df_0['Addons'].map(
        lambda a: False if a is None else 'VR' in a.text)
    df['PSVR2'] = df_0['Addons'].map(
        lambda a: False if a is None else 'VR2' in a.text)
    df['Platform'] = platform
    
    # Create a hash to uniquely identify the entry
    df['Hash'] = (df['Title'] + df['Developer'] + df['Publisher'] + df[
        'Platform'])
    df['Hash'] = df['Hash'].map(lambda a: re.sub(r'\W+', '', a).lower())
    df['Hash'] = df['Hash'].map(
        lambda a: hashlib.sha256(a.encode('utf-8')).hexdigest())
    
    return df


@task(
    task_id="wikipedia_scrape_ps4_al_page",
    retries=2,
)
def retrieve_ps4_al_page():
    return get_page(url_wikipedia_ps4_al)


@task(
    task_id="wikipedia_scrape_ps4_mz_page",
    retries=2,
)
def retrieve_ps4_mz_page():
    return get_page(url_wikipedia_ps4_mz)


@task(
    task_id="wikipedia_retrieve_ps5_page",
    retries=2,
)
def retrieve_ps5_page():
    return get_page(url_wikipedia_ps5)


@task(
    task_id="wikipedia_parse_ps4_al",
    outlets=[WIKIPEDIA_PS4_A],
)
def parse_ps4_al(page):
    bs_page = BeautifulSoup(page, 'html.parser')
    df = parse_wikipedia_list(bs_page, 'PS4')
    df.to_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_ps4_a.pkl')


@task(
    task_id="wikipedia_parse_ps4_mz",
    outlets=[WIKIPEDIA_PS4_M],
)
def parse_ps4_mz(page):
    bs_page = BeautifulSoup(page, 'html.parser')
    df = parse_wikipedia_list(bs_page, 'PS4')
    df.to_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_ps4_m.pkl')


@task(
    task_id="wikipedia_parse_ps5",
    outlets=[WIKIPEDIA_PS5],
)
def parse_ps5(page):
    bs_page = BeautifulSoup(page, 'html.parser')
    df = parse_wikipedia_list(bs_page, 'PS5')
    df.to_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_ps5.pkl')


@task(
    task_id="wikipedia_merge_results",
    outlets=[WIKIPEDIA_RESULT]
)
def merge_results():
    # Load datasets
    a = pd.read_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_ps4_a.pkl')
    b = pd.read_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_ps4_m.pkl')
    c = pd.read_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_ps5.pkl')
    
    # Merge lists
    df = a.merge(b, how='outer')
    df = df.merge(c, how='outer')
    df = df.sort_values(by=['Title'])
    df.to_pickle(f'{AIRFLOW_PATH}/datastore/wikipedia_result.pkl')
    

# Creating TaskGroups
@task_group(
    group_id='wikipedia_task_group'
)
def wikipedia_task_group():
    """TaskGroup for grouping related Tasks"""
    [
        parse_ps4_al(retrieve_ps4_al_page()),
        parse_ps4_mz(retrieve_ps4_mz_page()),
        parse_ps5(retrieve_ps5_page()),
    ] >> merge_results()
