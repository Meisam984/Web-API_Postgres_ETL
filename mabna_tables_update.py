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

# Obtain sensitive values from Enivornmental Variables
mabna_token = os.getenv("MABNA_TOKEN")

# Default DAG arguments
default_args = {
    'owner': 'sam',
    'start_date': datetime(2023, 3, 1),
    'email': ['m.rezaei@ehsan-group.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# Load YAML collections from constants.yaml file
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

# Divide updating tables according to the trades tables and others
def update_tables_body(url, title, item, params, max, params_tail="", trades_type=""):
    engine = postgres_connect()

    try:
        url = f"{url}/{title}/{item}?{params[0]}{max}{params[1]}{params_tail}"
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
                      con=engine, if_exists='append', index=False, schema='source')
            print(f"Successfully fetched new data and added to the table 'src_{title}_{item}{trades_type}' with {len(df)} rows")
        except Exception as e:
            print(f"Failed to add new data to the table 'src_{title}_{item}{trades_type}': {str(e)}")

    except requests.exceptions.RequestException as error:
        print(f"Error fetching data from the API URL: {str(error)}")


# Grab all the tables names, given a schema, together with their max 'meta.version' value in form of a dictionary
@task()
def max_meta_versions(schema):
    engine = postgres_connect()

    sql_tables = f"""SELECT table_name 
                     FROM information_schema.tables 
                     WHERE table_schema='{schema}'
                  """
    try:
        result = engine.execute(sql_tables)
        all_tables = []
        for r in result:
            all_tables.append(r[0])
        print(f"Successfully grabed all table names from '{schema}' schema")
    except Exception as e:
        print(f"Failed to grab all table names from '{schema}' schema: {str(e)}")

    tables_max_meta_versions = {}
    for table in all_tables:
        sql_meta_version = f"""SELECT MAX("meta.version") 
                               FROM "{schema}".{table}
                            """
        try:
            result = engine.execute(sql_meta_version)
            for v in result:
                tables_max_meta_versions[table] = v[0]
            print(f"Successfully grabed max 'meta.version' for {schema}.{table}: {v[0]} ")
        except Exception as e:
            print(f"Failed to grab max 'meta.version' for {schema}.{table}: {str(e)}")
    return tables_max_meta_versions


# Connect to the API URL and tables creation tempelate
@task()
def update_src_tables(max_meta_versions: dict):
    engine = postgres_connect()
    constants = yaml_load()

    url = constants['api_url']
    params = constants['api_parameters']['update']
    inst_types = constants['instrument_types']

    for title in list(constants['collection'].keys()):
        if title == 'exchange':
            for item in constants['collection']['exchange']:
                if item == 'trades':
                    for type in inst_types:
                        max_meta_version = max_meta_versions[f'src_exchange_trades_{type}']
                        update_tables_body(url=url, title=title, item=item, params=params, max=max_meta_version,
                                           params_tail=f"&instrument.type={type}",
                                           trades_type=f"_{type}")
                elif item in ['news', 'indexvalues']:
                    max_meta_version = max_meta_versions[f'src_exchange_{item}']
                    update_tables_body(url=url, title=title, item=item, params=params, max=max_meta_version)


# Remove null fields and converting date_time field to standard jalali format
@task()
def update_stg_trades(max_meta_versions: dict):
    engine = postgres_connect()
    constants = yaml_load()

    inst_types = constants['instrument_types']

    for type in inst_types:
        try:
            df = pd.read_sql_query(sql=f"""SELECT *
                                           FROM src_exchange_trades_{type}
                                           WHERE "meta.version" > {max_meta_versions[f'stg_exchange_trades_{type}']}
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
                df_stg.to_sql(name=f'stg_exchange_trades_{type}', con=engine, if_exists='append', index=False,
                              schema='staging')
                print(f"Succefully updated stg_exchange_trades_{type} table with {len(df_stg)} rows.")
            except Exception as err:
                print(f"Error adding new data to sql table stg_exchange_trades_{type}: {err}")
        except Exception as e:
            print(f"Error occured implementing read sql query from src_exchange_trades_{type}: {e}")


@task()
def update_stg_news(max_meta_versions: dict):
    engine = postgres_connect()

    try:
        df = pd.read_sql_query(sql=f"""SELECT *
                                       FROM src_exchange_news
                                       WHERE "meta.version" > {max_meta_versions['stg_exchange_news']}
                                    """,
                               con=engine)

        df_stg = df[['id', 'date_time', 'title', 'text', 'meta.version']]

        df_stg.dropna(subset=['date_time', 'title', 'text'], inplace=True)
        df_stg.insert(loc=2, column='j_date', value=df_stg['date_time'].str[:4] + '/' + \
                                                    df_stg['date_time'].str[4:6] + '/' + df_stg['date_time'].str[6:8])

        try:
            df_stg.to_sql(name=f'stg_exchange_news', con=engine, if_exists='append', index=False, schema='staging')
            print(f"""Succefully updated  stg_exchange_news table with {len(df_stg)} rows.""")
        except Exception as err:
            print(f"Error storing new data to sql table stg_exchange_news: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from src_exchange_news: {e}")


@task()
def update_stg_indexvalues(max_meta_versions: dict):
    engine = postgres_connect()

    try:
        df = pd.read_sql_query(sql=f"""SELECT *
                                       FROM src_exchange_indexvalues
                                       WHERE "meta.version" > {max_meta_versions['stg_exchange_indexvalues']}
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
            df_stg.to_sql(name=f'stg_exchange_indexvalues', con=engine, if_exists='append', index=False,
                          schema='staging')
            print(f"Succefully updated stg_exchange_indexvalues table with {len(df_stg)} rows.")
        except Exception as err:
            print(f"Error adding new data to sql table stg_exchange_indexvalues: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from src_exchange_indexvalues: {e}")




# Join some instrument fields to the trades tables
@task()
def update_prd_trades(max_meta_versions: dict):
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
            	                               WHERE {type}."meta.version" > {max_meta_versions[f'prd_exchange_trades_{type}']}
                                            """,
                                       con=engine)
            if type in ['commodity', 'currency']:
                df_prd.drop(columns=['code', 'isin', 'trade_count', 'volume', 'value'], inplace=True)
            try:
                df_prd.to_sql(name=f'prd_exchange_trades_{type}',
                              con=engine, if_exists='append', index=False, schema='production')
                print(f"Succefully added new data to prd_exchange_trades_{type} table with {len(df_prd)} rows.")
            except Exception as err:
                print(f"Error adding new data to sql table prd_exchange_trades_{type}: {err}")
        except Exception as e:
            print(f"Error occured implementing read sql query from stg_exchange_trades_{type}: {e}")

# Drop duplicate data with respect to j_date and name
@task()
def drop_duplicates_prd_trades():
    engine = postgres_connect()
    constants = yaml_load()

    inst_types = constants['instrument_types']

    for type in inst_types:
        try:
            df = pd.read_sql_query(sql=f"""SELECT *
                                           FROM prd_exchange_trades_{type}
                                           ORDER BY "meta.version"
                                        """,
                                   con=engine)

            df.drop_duplicates(subset=["j_date", "name"], keep='last', inplace=True)
            try:
                df.to_sql(name=f'prd_exchange_trades_{type}', con=engine, if_exists='replace', index=False,
                              schema='production')
                print(f"Succefully dropped duplicated data from prd_exchange_trades_{type} table.")
            except Exception as err:
                print(f"Error storing latest data to sql table prd_exchange_trades_{type}: {err}")

        except Exception as e:
            print(f"Failed to fetch data from prd_exchange_trades_{type}")


@task()
def update_prd_news(max_meta_versions: dict):
    engine = postgres_connect()
    try:
        df_prd = pd.read_sql_query(sql=f"""SELECT *
                                           FROM stg_exchange_news
                                           WHERE "meta.version" > {max_meta_versions['prd_exchange_news']}
                                        """,
                                   con=engine)
        df_prd = df_prd[['id', 'j_date', 'title', 'text', 'meta.version']]
        try:
            df_prd.to_sql(name=f'prd_exchange_news',
                          con=engine, if_exists='append', index=False, schema='production')
            print(f"Succefully added new data to prd_exchange_news table with {len(df_prd)} rows.")
        except Exception as err:
            print(f"Error adding new data to sql table prd_exchange_news: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from stg_exchange_news: {e}")


# Drop duplicate data with respect to date and title
@task()
def drop_duplicates_prd_news():
    engine = postgres_connect()

    try:
        df = pd.read_sql_query(sql=f"""SELECT *
                                       FROM prd_exchange_news
                                       ORDER BY "meta.version"
                                    """,
                               con=engine)

        df.drop_duplicates(subset=['j_date', 'title'], keep='last', inplace=True)
        try:
            df.to_sql(name='prd_exchange_news', con=engine, if_exists='replace', index=False,
                          schema='production')
            print("Succefully dropped duplicated data from prd_exchange_news table.")
        except Exception as err:
            print(f"Error storing latest data to sql table prd_exchange_news: {err}")

    except Exception as e:
        print("Failed to fetch data from prd_exchange_news.")


@task()
def update_prd_indexvalues(max_meta_versions: dict):
    engine = postgres_connect()

    try:
        df_prd = pd.read_sql_query(sql=f"""SELECT 
        	                                    indexvalues.id, j_date, indexes.name, 
        	                                    open_value, low_value, high_value, close_value, close_value_change,
        	                                    close_value_change_percent, indexvalues."meta.version"
                                           FROM stg_exchange_indexvalues AS indexvalues
                                           JOIN src_exchange_indexes AS indexes
        	                                    ON indexvalues."index.id" = indexes.id
        	                               WHERE indexvalues."meta.version" > {max_meta_versions['prd_exchange_indexvalues']}
                                        """,
                                   con=engine)
        try:
            df_prd.to_sql(name=f'prd_exchange_indexvalues',
                          con=engine, if_exists='append', index=False, schema='production')
            print(f"Succefully added new data to prd_exchange_indexvalues table with {len(df_prd)} rows.")
        except Exception as err:
            print(f"Error adding new data to sql table prd_exchange_indexvalues: {err}")
    except Exception as e:
        print(f"Error occured implementing read sql query from stg_exchange_indexvalues: {e}")


# Drop duplicate data with respect to date and name
@task()
def drop_duplicates_prd_indexvalues():
    engine = postgres_connect()

    try:
        df = pd.read_sql_query(sql=f"""SELECT *
                                       FROM prd_exchange_indexvalues
                                       ORDER BY "meta.version"
                                    """,
                               con=engine)

        df.drop_duplicates(subset=['j_date', 'name'], keep='last', inplace=True)
        try:
            df.to_sql(name='prd_exchange_indexvalues', con=engine, if_exists='replace', index=False,
                          schema='production')
            print("Succefully dropped duplicated data from prd_exchange_indexvalues table.")
        except Exception as err:
            print(f"Error storing latest data to sql table prd_exchange_indexvalues: {err}")

    except Exception as e:
        print("Failed to fetch data from prd_exchange_indexvalues.")


# DAG definition
with DAG(dag_id='Mabna_ETL_Daily', default_args=default_args, schedule="*/15 11-20 * * 0-6") as dag:
    with TaskGroup(group_id='update_source_tables', tooltip='update Lookup and Data tables') as update_source_tables:
        check_max_meta_version_src_tables = max_meta_versions('source')
        src_tables_update = update_src_tables(check_max_meta_version_src_tables)
        # Order
        check_max_meta_version_src_tables >> src_tables_update

    with TaskGroup(group_id='update_stg_tables', tooltip='update and transform scr tables to staging schema') \
         as update_staging_tables:
            update_staging_trades = update_stg_trades(max_meta_versions("staging"))
            update_staging_news = update_stg_news(max_meta_versions("staging"))
            update_staging_indexvalues = update_stg_indexvalues(max_meta_versions("staging"))
            # Order
            [update_staging_trades, update_staging_news, update_staging_indexvalues]

    with TaskGroup(group_id='update_prd_tables', tooltip='update and transform stg tables to production schema') \
         as update_production_tables:
            update_production_trades = update_prd_trades(max_meta_versions("production"))
            update_production_news = update_prd_news(max_meta_versions("production"))
            update_production_indexvalues = update_prd_indexvalues(max_meta_versions("production"))
            drop_dup_prd_trades = drop_duplicates_prd_trades()
            drop_dup_prd_news = drop_duplicates_prd_news()
            drop_dup_prd_indexvalues = drop_duplicates_prd_indexvalues()
            update_production_trades >> drop_dup_prd_trades
            update_production_news >> drop_dup_prd_news
            update_production_indexvalues >> drop_dup_prd_indexvalues

    update_source_tables >> update_staging_tables >> update_production_tables


























