from Redis import RedisDB
from PostgreSQL import PostgreSQLDB
import pandas as pd

databases = {
    'Redis': RedisDB,
    'PostgreSQL': PostgreSQLDB
}


def run_basic_tests(db_conn):
    test_times_df = pd.DataFrame({
        'test_select_index' : [db_conn.test_select_index(index='qkRM_2X51Yqxk3btlwAQIg')],
        'test_select_where' : [db_conn.test_select_where(key='name', value='The UPS Store')],
        'test_insert_1' : [db_conn.test_insert_1()],
        'test_insert_100' : [db_conn.test_insert_100()],
        'test_delete_index' : [db_conn.test_delete_index(index='qkRM_2X51Yqxk3btlwAQIg')],
        'test_delete_where' : [db_conn.test_delete_where(key='name', value='The UPS Store')],
        'test_update_index' : [db_conn.test_update_index(search_index='bBDDEgkFA1Otx9Lfe7BZUQ', update_key='stars', update_value=1)],
        'test_update_where' : [db_conn.test_update_where(search_key='is_open', search_value=0, update_key='stars', update_value=0)]
    })
    
    return test_times_df


if __name__ == '__main__':
    db_type = 'Redis'
    table_size = 100
    
    results_df = run_basic_tests(databases[db_type](table_size))
    results_df['database'] = db_type
    results_df['table_size'] = table_size
    