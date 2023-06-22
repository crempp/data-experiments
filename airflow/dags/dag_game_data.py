from airflow.decorators import dag
import pendulum
from gamedata.wikipedia import wikipedia_task_group
from gamedata.metacritic import metacritic_scrape


@dag(
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gamedata"],
)
def game_data_pipeline():
    """
    ### GameData Pipeline Documentation
    """
    wikipedia_task_group()
    #>> metacritic_scrape()


dag_object = game_data_pipeline()

if __name__ == "__main__":
    dag_object.test()
