# Import required libraries
from datetime import datetime,timedelta
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import pandas as pd
import yaml
import requests
import os
import jdatetime

# Obtain sensitive values from Enivornmental Variables
mabna_token = os.getenv("MABNA_TOKEN")

# Today date in 14011225000000 format
fa_today = jdatetime.date.today().strftime("%Y/%m/%d")

# Default DAG arguments
default_args = {
    'owner': 'sam',
    'start_date': datetime(2023, 2, 26),
    'email': ['m.rezaei@ehsan-group.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# Loading YAML collections from constants.yaml file
def yaml_load():
    with open('/constants.yaml', 'r') as f:
        document = yaml.full_load(f)
        constants = dict(document.items())
        return  constants

# Establish a connection to postgresql database 'mabna'
def postgres_connect():
    conn = BaseHook.get_connection("mabna_postgres")
    engine = create_engine(f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    return engine

# Divide creating tables according to the trades tables and others
def tables_create_body(url, title, item, params, params_tail="", trades_type=""):
    engine = postgres_connect()
    try:
        url = f"{url}/{title}/{item}?{params[0]}{params[1]}{params_tail}"
        payload = {}
        headers = {
            'Authorization': mabna_token
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()
        data = data['data']
        df = pd.json_normalize(data)
        try:
            df.to_sql(name=f"src_{title}_{item}{trades_type}",
                      con=engine, if_exists='replace', index=False, schema='source')
            print(f"Successfully fetched data and created table 'src_{title}_{item}{trades_type}' with {len(df)} rows")
        except Exception as e:
            print(f"Failed to create table 'src_{title}_{item}{trades_type}': {str(e)}")

    except requests.exceptions.RequestException as error:
        print(f"Error fetching data from the API URL: {str(error)}")


# Connecting to the API URL and tables creation tempelate
@task()
def create_src_tables():
    engine = postgres_connect()
    constants = yaml_load()

    url = constants['api_url']
    params = constants['api_parameters']['create']
    inst_types = constants['instrument_types']

    for title in list(constants['collection'].keys()):
        for item in constants['collection'][title]:
            if item == 'trades':
                for type in inst_types:
                    # Creating trades tables with thye format: "src_title_item_type"
                    tables_create_body(url=url, title=title, item=item, params=params,
                                       params_tail=f"&instrument.type={type}",
                                       trades_type=f"_{type}")
            else:
                # Creating other tables with the format: "src_title_item"
                tables_create_body(url=url, title=title, item=item, params=params)

@task()
def create_assets_table():
    engine = postgres_connect()
    constants = yaml_load()

    url = constants['api_url']
    params = constants['api_parameters']['create']

    try:
        url = f"{url}/exchange/assets?{params[0]}{params[1]}"
        payload = {}
        headers = {
            'Authorization': mabna_token
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()
        data = data['data']
        df = pd.json_normalize(data)

        df.dropna(subset=['categories'], inplace=True)
        list = []
        for item in df['categories'].items():
            list.append(item[1][0]['id'])
        ds = pd.Series(list)
        df.insert(loc=8, column='category.id', value=ds)
        df.drop(['categories'], axis=1, inplace=True)

        try:
            df.to_sql(name=f"src_exchange_assets",
                      con=engine, if_exists='replace', index=False, schema='source')
            print(f"Successfully fetched data and created table 'src_exchange_assets' with {len(df)} rows")
        except Exception as e:
            print(f"Failed to create table 'src_exchange_assets': {str(e)}")
    except requests.exceptions.RequestException as error:
        print(f"Error fetching data from the API URL: {str(error)}")



# Removing null fields and converting date_time field to standard jalali format
@task()
def transform_src_trades():
    engine = postgres_connect()
    constants = yaml_load()

    inst_types = constants['instrument_types']

    for type in inst_types:
        try:
            df = pd.read_sql_query(sql=f"""SELECT *
                                           FROM src_exchange_trades_{type}
                                        """,
                                   con=engine)
            df_stg = df[['id', 'date_time', 'open_price', 'high_price', 'low_price', 'close_price', 'close_price_change',
                         'trade_count', 'volume', 'value', 'instrument.id', 'meta.version']]
            df_stg.dropna(subset=['date_time', 'open_price', 'high_price', 'low_price', 'close_price', 'close_price_change',
                                  'trade_count', 'volume', 'value', 'instrument.id'], inplace=True)
            df_stg.insert(loc=2, column='j_date',
                          value=df_stg['date_time'].str[:4] + '/' + \
                                df_stg['date_time'].str[4:6] + '/' + \
                                df_stg['date_time'].str[6:8])
            df_stg.insert(loc=8, column='close_price_change_percent',
                          value=df_stg['close_price_change'] / (df_stg['close_price']- df_stg['close_price_change']))
            try:
                df_stg.to_sql(name=f'stg_exchange_trades_{type}', con=engine, if_exists='replace', index=False,
                              schema='staging')
                print(f"Succefully transformed src_exchange_trades_{type} table to the staging schema with {len(df_stg)} rows.")
            except Exception as err:
                print(f"Error storing dataframe to sql table stg_exchange_trades_{type}: {err}")
        except Exception as e:
            print(f"Error occured implementing read sql query from src_exchange_trades_{type}: {e}")

@task()
def transform_src_news():
    engine = postgres_connect()

    try:
        df = pd.read_sql_query(sql=f"""SELECT *
                                       FROM src_exchange_news
                                    """,
                               con=engine)
        df_stg = df[['id', 'date_time', 'title', 'text', 'meta.version']]

        df_stg.dropna(subset=['date_time', 'title', 'text'], inplace=True)
        df_stg.insert(loc=2, column='j_date', value=df_stg['date_time'].str[:4] + '/' + \
                                                    df_stg['date_time'].str[4:6] + '/' + \
                                                    df_stg['date_time'].str[6:8])

        try:
            df_stg.to_sql(name=f'stg_exchange_news', con=engine, if_exists='replace', index=False, schema='staging')
            print(f"Succefully transformed src_exchange_news table to the staging schema with {len(df_stg)} rows.")
        except Exception as err:
            print(f"Error storing dataframe to sql table stg_exchange_news: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from src_exchange_news: {e}")

@task()
def transform_src_indexvalues():
    engine = postgres_connect()

    try:
        df = pd.read_sql_query(sql=f"""SELECT *
                                       FROM src_exchange_indexvalues
                                    """,
                               con=engine)
        df_stg = df[['id', 'date_time', 'open_value', 'low_value', 'high_value', 'close_value', 'close_value_change',
                     'index.id', 'meta.version']]

        df_stg.dropna(subset=['date_time', 'open_value', 'low_value', 'high_value', 'close_value', 'close_value_change',
                              'index.id'], inplace=True)
        df_stg.insert(loc=2, column='j_date',
                      value=df_stg['date_time'].str[:4] + '/' + \
                            df_stg['date_time'].str[4:6] + '/' + \
                            df_stg['date_time'].str[6:8])
        df_stg.insert(loc=8, column='close_value_change_percent',
                      value=df_stg['close_value_change'] / (df_stg['close_value']- df_stg['close_value_change']))
        try:
            df_stg.to_sql(name=f'stg_exchange_indexvalues', con=engine, if_exists='replace', index=False, schema='staging')
            print(f"Succefully transformed src_exchange_indexvalues table to the staging schema with {len(df_stg)} rows.")
        except Exception as err:
            print(f"Error storing dataframe to sql table stg_exchange_indexvalues: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from src_exchange_indexvalues: {e}")

# Join some instrument fields to the trades tables
@task()
def create_prd_trades():
    engine = postgres_connect()
    constants = yaml_load()

    inst_types = constants['instrument_types']

    for type in inst_types:
        try:
            df_prd = pd.read_sql_query(sql=f"""SELECT
                                                    {type}.id, 
                                                    j_date, instruments.code, instruments.isin, instruments.name, 
                                                    instruments."stock.company.id" AS "company.id",
                                                    categories.short_name AS category, categories.id AS "category.id",
                                                    exchanges.title AS market, exchanges.id AS "market.id",
                                                    open_price, high_price, low_price, close_price,
                                                    close_price_change, close_price_change_percent,
                                                    trade_count, volume, value, {type}."meta.version"
                                               FROM stg_exchange_trades_{type} AS {type}
                                               JOIN src_exchange_instruments AS instruments
            	                                    ON {type}."instrument.id" = instruments.id
            	                               JOIN src_exchange_assets AS  assets
            	                                    ON assets.id = instruments."asset.id"
            	                               JOIN src_exchange_categories AS categories
            	                                    ON  categories.id = assets."category.id"
            	                               JOIN src_exchange_exchanges AS exchanges
            	                                    ON exchanges.id = instruments."exchange.id"
            	                               WHERE j_date BETWEEN '1399/01/01' AND '{fa_today}'
                                            """,
                                       con=engine)
            if type in ['commodity', 'currency']:
                df_prd.drop(columns=['code', 'isin', 'trade_count', 'volume', 'value'], inplace=True)
            try:
                df_prd.to_sql(name=f'prd_exchange_trades_{type}',
                              con=engine, if_exists='replace', index=False, schema='production')
                print(f"Succefully created prd_exchange_trades_{type} table with {len(df_prd)} rows.")
            except Exception as err:
                print(f"Error storing dataframe to sql table prd_exchange_trades_{type}: {err}")
        except Exception as e:
            print(f"Error occured implementing read sql query from stg_exchange_trades_{type}: {e}")

@task()
def create_prd_news():
    engine = postgres_connect()
    try:
        df_prd = pd.read_sql_query(sql=f"""SELECT *
                                           FROM stg_exchange_news
                                           WHERE j_date BETWEEN '1399/01/01' AND '{fa_today}'
                                        """,
                                   con=engine)
        df_prd = df_prd[['id', 'j_date', 'title', 'text', 'meta.version']]
        try:
            df_prd.to_sql(name=f'prd_exchange_news',
                      con=engine, if_exists='replace', index=False, schema='production')
            print(f"Succefully created prd_exchange_news table with {len(df_prd)} rows.")
        except Exception as err:
            print(f"Error storing dataframe to sql table prd_exchange_news: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from stg_exchange_news: {e}")

@task()
def create_prd_indexvalues():
    engine = postgres_connect()

    try:
        df_prd = pd.read_sql_query(sql=f"""SELECT 
        	                                    indexvalues.id, j_date, indexes.name, 
        	                                    open_value, low_value, high_value, close_value, close_value_change,
        	                                    close_value_change_percent, indexvalues."meta.version"
                                           FROM stg_exchange_indexvalues AS indexvalues
                                           JOIN src_exchange_indexes AS indexes
        	                                    ON indexvalues."index.id" = indexes.id
        	                               WHERE j_date BETWEEN '1399/01/01' AND '{fa_today}'
                                        """,
                                   con=engine)
        try:
            df_prd.to_sql(name=f'prd_exchange_indexvalues',
                          con=engine, if_exists='replace', index=False, schema='production')
            print(f"Succefully created prd_exchange_indexvalues table with {len(df_prd)} rows.")
        except Exception as err:
            print(f"Error storing dataframe to sql table prd_exchange_indexvalues: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from stg_exchange_indexvalues: {e}")


# DAG definition
with DAG(dag_id='Mabna_ETL_Creation', default_args=default_args, schedule_interval=None) as dag:
    with TaskGroup(group_id='create_src_tables', tooltip='Create Lookup and Data tables') as extract:
        src_tables_creation = create_src_tables()
        asst_table_creation = create_assets_table()
        # Order
        [src_tables_creation, asst_table_creation]
    with TaskGroup(group_id='transform_trades_news_indexvalues_tables', tooltip='Transform to staging schema') as transform:
        trades_transform = transform_src_trades()
        news_transform = transform_src_news()
        indexvalues_transform = transform_src_indexvalues()
        # Order
        [trades_transform, news_transform, indexvalues_transform]
    with TaskGroup(group_id='create_prd_tables', tooltip='Create production schema tables') as load:
        prd_trades_tables_creation = create_prd_trades()
        prd_news_table_creation = create_prd_news()
        prd_indexvalues_table_creation = create_prd_indexvalues()
        #Order
        [prd_trades_tables_creation, prd_news_table_creation, prd_indexvalues_table_creation]

    extract >> transform >> load
























