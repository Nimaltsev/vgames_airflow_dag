import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

#import json
#from urllib.parse import urlencode


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

data_link =  '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = int(1994 + hash(f"{'n-maltsev'}") % 23)

default_args = {
    'owner': 'n-maltsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 10)
}

# CHAT_ID = 37127180
# try:
#     BOT_TOKEN = Variable.get('telegram_secret')
# except:
#     BOT_TOKEN = ''

# def send_message(context):
#     date = context['ds']
#     dag_id = context['dag'].dag_id
#     message = f'Huge success! Dag {dag_id} completed on {date}'
#     if BOT_TOKEN != '':
#         bot = telegram.Bot(token=BOT_TOKEN)
#         bot.send_message(chat_id=CHAT_ID, text=message)
#     else:
#         pass

@dag(default_args=default_args, schedule_interval= '0 18 * * *', catchup=False)
def n_maltsev_dag():
# Getting data
    @task(retries=3)
    def get_data():
        data_link =  '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
        vgsales_data = pd.read_csv(data_link).dropna(subset=['Year']).astype({'Year': 'int32'})
        year = int(1994 + hash(f"{'n-maltsev'}") % 23)
        vgsales_data = vgsales_data.query('Year == @year')
        return vgsales_data
#  Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=4, retry_delay=timedelta(10))
    def get_global_top_game(vgsales_data):
        global_top_game = vgsales_data.sort_values('Global_Sales', ascending = False).head(1).iloc[0]['Name']
        return {'global_top_game':global_top_game}
    
# Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def get_top_european_genre(vgsales_data):
        top_european_genre = vgsales_data.groupby('Genre', as_index = False)['EU_Sales']\
            .sum()\
            .sort_values('EU_Sales', ascending = False)\
            .head(1)\
            .iloc[0]['Genre']
        return {'top_european_genre':top_european_genre}
#На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
#Перечислить все, если их несколько
 
    @task()
    def get_NA_top_platform(vgsales_data):
        NA_top_platform = vgsales_data.query('NA_Sales > 1')\
            .groupby('Platform', as_index=False)['NA_Sales']\
            .nunique()\
            .sort_values('NA_Sales', ascending=False)\
            .head(1)\
            .iloc[0]['Platform']
        return {'NA_top_platform':NA_top_platform}
# У какого издателя самые высокие средние продажи в Японии?
# Перечислить все, если их несколько
    @task()
    def get_top_japan_publisher(vgsales_data):
        top_japan_publisher = vgsales_data\
            .groupby('Publisher', as_index=False)['JP_Sales']\
            .mean().sort_values('JP_Sales', ascending=False)\
            .head(1)\
            .iloc[0]['Publisher']
        return {'top_japan_publisher':top_japan_publisher}
# Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def get_EU_over_JP_count(vgsales_data):
        EU_over_JP_count = vgsales_data.query('EU_Sales>JP_Sales').Name.nunique()    
        return {'EU_over_JP_count':EU_over_JP_count}
    
# Выводим данные 
# on_success_callback=send_message

    @task()
    def print_data(stat1, stat2, stat3, stat4, stat5):

        context = get_current_context()
        date = context['ds']
        
        global_top_game = stat1['global_top_game']
        top_european_genre = stat2['top_european_genre']
        NA_top_platform = stat3['NA_top_platform']
        top_japan_publisher = stat4['top_japan_publisher']
        EU_over_JP_count = stat5['EU_over_JP_count']
        
        year = int(1994 + hash(f"{'n-maltsev'}") % 23)
        print(f'''Data for {year}
                  The best selling game in the world: {global_top_game}
                  The best selling game genre  : {top_european_genre}
                  Platform that has had the most games that have been sold over a million copies in North America : {NA_top_platform}
                  The publisher which has had the highest average sales in Japan : {top_japan_publisher}
                  Count of games which have beeen sold in Europe better than in Japan: {EU_over_JP_count}
                  ''')

    vgsales_data = get_data()
    global_top_game = get_global_top_game(vgsales_data)
    top_european_genre = get_top_european_genre(vgsales_data)
    NA_top_platform = get_NA_top_platform(vgsales_data)
    top_japan_publisher = get_top_japan_publisher(vgsales_data)
    EU_over_JP_count = get_EU_over_JP_count(vgsales_data)
    print_data(global_top_game, top_european_genre, NA_top_platform, top_japan_publisher, EU_over_JP_count)

n_maltsev_dag = n_maltsev_dag()