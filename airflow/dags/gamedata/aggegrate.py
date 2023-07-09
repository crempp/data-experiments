import logging
from airflow import Dataset
from airflow.decorators import task
from gamedata.helpers import get_s3_file, put_s3_file, s3_path_exists


HEADERS = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
BASE_METACRITIC_URL = 'https://www.metacritic.com/game'

PATH_WIKIPEDIA_RESULT = f'wikipedia_result.csv'
PATH_METACRITIC_RESULT = f'metacritic_result.csv'
DS_METACRITIC_RESULT = Dataset(f'lorenz://datalake/{PATH_METACRITIC_RESULT}')

logger = logging.getLogger(__name__)



@task(
    task_id="metacritic_scrape",
    outlets=[DS_METACRITIC_RESULT]
)
def aggegrate():
    pass
