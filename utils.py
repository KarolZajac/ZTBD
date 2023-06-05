import pandas as pd

db_params = {
    "Yelp": {
        "index": "qkRM_2X51Yqxk3btlwAQIg",
        "key": 'name',
        "value": 'The UPS Store',
        "search_index": 'bBDDEgkFA1Otx9Lfe7BZUQ',
        "update_key": 'stars',
        "update_value": 2,
        "search_key": 'is_open',
        "search_value": 0,
        "column": 'stars',
        "search_column": 'categories',
        "string": 'Food'
    },
    "IMDB": {
        "index": "qkRM_2X51Yqxk3btlwAQIg",
        "key": 'name',
        "value": 'The UPS Store',
        "search_index": 'bBDDEgkFA1Otx9Lfe7BZUQ',
        "update_key": 'stars',
        "update_value": 2,
        "search_key": 'is_open',
        "search_value": 0,
        "column": 'stars',
        "search_column": 'categories',
        "string": 'Food'
    }
}


def run_basic_tests(db_conn, db_params):
    test_times_df = pd.DataFrame({
        'test_select_index': [db_conn.test_select_index(index=db_params["index"])],
        'test_select_where': [db_conn.test_select_where(key=db_params["key"], value=db_params["value"])],
        'test_insert_1': [db_conn.test_insert_1()],
        'test_insert_many': [db_conn.test_insert_many()],
        'test_delete_index': [db_conn.test_delete_index(index=db_params["index"])],
        'test_delete_where': [db_conn.test_delete_where(key=db_params["key"], value=db_params["value"])],
        'test_update_index': [
            db_conn.test_update_index(search_index=db_params["search_index"], update_key=db_params["update_key"],
                                      update_value=db_params["update_value"])],
        'test_update_where': [
            db_conn.test_update_where(search_key=db_params["search_key"], search_value=db_params["search_value"],
                                      update_key=db_params["update_key"], update_value=db_params["update_value"])],
        'test_column_avg': [db_conn.test_column_avg(column=db_params["column"])],
        'test_column_median': [db_conn.test_column_median(column=db_params["column"])],
        # 'test_distribution': [db_conn.test_distribution(key='stars')],
        'test_count_word_occurences': [
            db_conn.test_count_word_occurences(search_column=db_params["search_column"], string=db_params["string"])]
    })

    return test_times_df
