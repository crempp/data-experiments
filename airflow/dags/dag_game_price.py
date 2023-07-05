from airflow.decorators import dag
import pendulum
from gamedata.ps_direct_physical import ps_direct_physical_scrape
from gamedata.ps_digital import ps_digital_scrape


@dag(
    schedule='0 */6 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gameprice"],
)
def game_price_pipeline():
    """
    ### Game Price Pipeline Documentation
    """
    ps_direct_physical_scrape()
    
    # Do not run digital scrape at the moment.
    # store.playstation.com bans hard
    #ps_digital_scrape()


dag_object = game_price_pipeline()

if __name__ == "__main__":
    dag_object.test()
