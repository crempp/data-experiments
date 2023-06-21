from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import pathlib
import re
import requests
import time
from airflow import Dataset
from airflow.decorators import task


DELAY = 1  # seconds
BASE_URL = 'https://store.playstation.com'
LIST_BASE_URL = f'{BASE_URL}/en-us/pages/browse'
API_HASH = '16b0b76dac848b6e33ed088a5a3aedb738e51c9481c6a6eb6d6c1c1991ea1f39'
headers={"X-Psn-Store-Locale-Override":"en-US"}
AIRFLOW_PATH = os.path.normpath(str(pathlib.Path(__file__).parent.resolve()) + '../../../')

PS_DIGITAL_RESULT = Dataset(f'file:/{AIRFLOW_PATH}/datastore/ps_digital_result.pkl')

session = requests.Session()


def scrape_game_page(data):
    response = session.get(data['url'], headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    name = soup.select('h1[data-qa="mfe-game-title#name"]')
    name = name[0].text if len(name) > 0 else None
    rating = soup.select('img[data-qa="mfe-content-rating#ratingImage#image-no-js"]')
    rating = rating[0]['alt'] if len(rating) > 0 else None

    notice0 = soup.select('span[data-qa="mfe-compatibility-notices#notices#notice0#compatText"]')
    notice0 = notice0[0].text if len(notice0) > 0 else None
    notice1 = soup.select('span[data-qa="mfe-compatibility-notices#notices#notice1#compatText"]')
    notice1 = notice1[0].text if len(notice1) > 0 else None
    notice2 = soup.select('span[data-qa="mfe-compatibility-notices#notices#notice2#compatText"]')
    notice2 = notice2[0].text if len(notice2) > 0 else None
    notice3 = soup.select('span[data-qa="mfe-compatibility-notices#notices#notice3#compatText"]')
    notice3 = notice3[0].text if len(notice3) > 0 else None
    notice4 = soup.select('span[data-qa="mfe-compatibility-notices#notices#notice4#compatText"]')
    notice4 = notice4[0].text if len(notice4) > 0 else None
    notice5 = soup.select('span[data-qa="mfe-compatibility-notices#notices#notice5#compatText"]')
    notice5 = notice5[0].text if len(notice5) > 0 else None

    description = soup.select('p[data-qa="mfe-game-overview#description"]')
    description = description[0].text if len(description) > 0 else None

    platform =  soup.select('dd[data-qa="gameInfo#releaseInformation#platform-value"]')
    platform = platform[0].text if len(platform) > 0 else None
    release = soup.select('dd[data-qa="gameInfo#releaseInformation#releaseDate-value"]')
    release = release[0].text if len(release) > 0 else None
    publisher = soup.select('dd[data-qa="gameInfo#releaseInformation#publisher-value"]')
    publisher = publisher[0].text if len(publisher) > 0 else None
    genre = soup.select('dd[data-qa="gameInfo#releaseInformation#genre-value"] span')
    genre = genre[0].text if len(genre) > 0 else None
    voice_lang = soup.select('dd[data-qa="gameInfo#releaseInformation#voice-value"]')
    voice_lang = voice_lang[0].text if len(voice_lang) > 0 else None
    screen_lang = soup.select('dd[data-qa="gameInfo#releaseInformation#subtitles-value"]')
    screen_lang = screen_lang[0].text if len(screen_lang) > 0 else None

    data['name'] = name
    data['rating'] = rating
    data['notice0'] = notice0
    data['notice1'] = notice1
    data['notice2'] = notice2
    data['notice3'] = notice3
    data['notice4'] = notice4
    data['notice5'] = notice5
    data['description'] = description
    data['platform'] = platform
    data['release'] = release
    data['publisher'] = publisher
    data['genre'] = genre
    data['voice_lang'] = voice_lang
    data['screen_lang'] = screen_lang

    return data


def scrape_game_api(data):
    d = {}
    concept_id = data['concept_id']

    api_url = f'https://web.np.playstation.com/api/graphql/v1//op?operationName=queryRetrieveTelemetryDataPDPConcept&variables={{"conceptId":"{concept_id}","productId":null}}&extensions={{"persistedQuery":{{"version":1,"sha256Hash":"{API_HASH}"}}}}'
    response = session.get(api_url, headers=headers)
    j = json.loads(response.text)

    product = j['data']['conceptRetrieve']['defaultProduct']
    data['c_name'] = j['data']['conceptRetrieve']['name']
    data['c_rating'] = product['contentRating']['name']
    data['c_id'] = product['id']
    data['c_genres'] = list(map(lambda a: a['value'], product['localizedGenres']))
    data['c_default_prod_name'] = product['name']
    data['c_np_title_id'] = product['npTitleId']
    data['c_sku_id'] = product['skus'][0]['id']
    data['c_display_price'] = product['skus'][0]['displayPrice']
    data['c_price'] = product['skus'][0]['price']

    main_cta = list(filter(lambda a: a['price']['serviceBranding'][0] == 'NONE', product['webctas']))
    if len(main_cta) > 0:
        data['c_discounted_price'] = main_cta[0]['price']['discountedPrice']
    else:
        data['c_discounted_price'] = None

    return data

@task(
    task_id="ps_digital_scrape",
    retries=2,
    outlets=[PS_DIGITAL_RESULT]
)
def ps_digital_scrape():
    page = 1

    game_list = []

    # Loop through pages
    while True:
        # Retrieve list page
        response = session.get(f'{LIST_BASE_URL}/{page}')
        soup = BeautifulSoup(response.text, 'html.parser')

        # There is no clear way to know when we've arrived at the end.
        # I've found that the "All Games" header does not appear when
        # there are no more results.
        if len(soup.find_all("h1", string="All Games")) == 0:
            break

        # Get games on page
        games = soup.select('ul.psw-grid-list li')

        for game in games:
            data = {}
            path = game.select('div a')[0]['href']

            id_search = re.search('(\d*)$', path, re.IGNORECASE)
            if id_search:
                id = id_search.group(1)
            else:
                id = None

            data['concept_id'] = id
            data['url'] = f'{BASE_URL}/{path}'
            data['img'] = game.select('img.psw-l-fit-cover')[0]['src']

            print(f'id: {id}')
            print(f'url: {data["url"]}\n')

            data = scrape_game_page(data)
            data = scrape_game_api(data)
            game_list.append(data)

            time.sleep(DELAY)

        page = page + 1
    
    df = pd.DataFrame(game_list)
    df.to_pickle(f'{AIRFLOW_PATH}/datastore/ps_digital_result.pkl')
