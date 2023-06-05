import pandas as pd


def run_basic_tests(db_conn):
    test_times_df = pd.DataFrame({
        'test_select_index': [db_conn.test_select_index(index='qkRM_2X51Yqxk3btlwAQIg')],
        'test_select_where': [db_conn.test_select_where(key='name', value='The UPS Store')],
        'test_insert_1': [db_conn.test_insert_1()],
        'test_insert_many': [db_conn.test_insert_many()],
        'test_delete_index': [db_conn.test_delete_index(index='qkRM_2X51Yqxk3btlwAQIg')],
        'test_delete_where': [db_conn.test_delete_where(key='name', value='The UPS Store')],
        'test_update_index': [
            db_conn.test_update_index(search_index='bBDDEgkFA1Otx9Lfe7BZUQ', update_key='stars', update_value=1)],
        'test_update_where': [
            db_conn.test_update_where(search_key='is_open', search_value=0, update_key='stars', update_value=0)],
        'test_column_avg': [db_conn.test_column_avg(key='stars')],
        'test_column_stddev': [db_conn.test_column_stddev(key='stars')],
        'test_distribution': [db_conn.test_distribution(key='stars')],
        'test_count_word_occurences': [db_conn.test_count_word_occurences(key="categories", string='Food')]

    })

    return test_times_df
