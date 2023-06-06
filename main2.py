import pandas as pd

import utils
from PostgreSQL import PostgreSQLDB
databases = {

    'PostgreSQL': PostgreSQLDB
}
from utils import *


if __name__ == '__main__':
    db_type = 'PostgreSQL'
    table_size = 100

    results_df = run_basic_tests(databases[db_type](table_size, "Yelp"), db_params["Yelp"])
    results_df['database'] = db_type
    results_df['table_size'] = table_size
    print(results_df)
