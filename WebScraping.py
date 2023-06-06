import json
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
import sqlite3


def scrapeData(cursor):
    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    driver = webdriver.Chrome(chrome_options=options)
    driver.get("https://www.imdb.com/chart/top")
    movies = driver.find_elements(By.CSS_SELECTOR, 'td.titleColumn')
    ratings = driver.find_elements(By.CSS_SELECTOR, 'td.imdbRating strong')

    for movie, rating in zip(movies, ratings):
        title = movie.find_element(By.CSS_SELECTOR, 'a').text
        year = movie.find_element(By.CSS_SELECTOR, 'span.secondaryInfo').text.strip('()')
        rating_value = rating.text.strip()
        rating_value = rating_value.replace(',', '.')
        addToSQLite(cursor, title, year, rating_value)
    driver.quit()

def addToSQLite(cursor, title, year, rating_value):
    cursor.execute("INSERT INTO movies (title, year, rating) VALUES (?, ?, ?)",
                   (title, int(year), float(rating_value)))


def printDB(cursor):
    cursor.execute("SELECT * FROM yelp")
    rows = cursor.fetchall()
    print(rows)

# def fullTextSearch(cursor):
#     cursor.execute("CREATE VIRTUAL TABLE movies_fts USING FTS5(id, title, year, rating)")
#     cursor.execute("INSERT INTO movies_fts(id, title, year, rating) SELECT id, title, year, rating FROM movies")
#     search_query = "Alien"
#     cursor.execute("SELECT * FROM movies_fts WHERE movies_fts MATCH ?", (search_query,))
#     results = cursor.fetchall()
#     cursor.execute("SELECT * FROM movies_fts WHERE nazwa LIKE ?", ('%' + search_query + '%',))
#     print(results)

def testMatch(cursor):
    start = datetime.now()
    cursor.execute("SELECT * FROM yelp_fts WHERE yelp_fts MATCH ?", ("The UPS Store",))
    return datetime.now() - start




def main():
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()
    # cursor.execute('''CREATE TABLE IF NOT EXISTS movies
    #                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
    #                   title TEXT,
    #                   year INTEGER,
    #                   rating REAL)''')
    # scrapeData(cursor)
    # cursor.execute("CREATE VIRTUAL TABLE movies_fts USING FTS5(id, title, year, rating)")
    # cursor.execute("INSERT INTO movies_fts(id, title, year, rating) SELECT id, title, year, rating FROM movies")
    # print(f"Match time: {testMatch(cursor)}")
    # print(f"Like time: {testMatch(cursor)}")

    cursor.execute('''CREATE TABLE IF NOT EXISTS yelp
                      (business_id TEXT PRIMARY KEY,
                       name TEXT,
                       address TEXT,
                       city TEXT,
                       state TEXT,
                       postal_code TEXT,
                       latitude REAL,
                       longitude REAL,
                       stars REAL,
                       review_count INTEGER,
                       is_open INTEGER,
                       attributes TEXT,
                       categories TEXT,
                       hours TEXT)''')
    json_data = []

    with open('yelp_academic_dataset_business.json', 'r', encoding='utf-8') as file:
        for line in file:

            json_objects = line.strip().split('}{')

            for json_obj in json_objects:
                # Add necessary characters to create a valid JSON object
                if not json_obj.startswith('{'):
                    json_obj = '{' + json_obj
                if not json_obj.endswith('}'):
                    json_obj = json_obj + '}'

                # Handle empty objects or fragments
                if json_obj.strip() == '{}':
                    continue

                json_data.append(json.loads(json_obj))

    for item in json_data:
        business_id = item.get('business_id')
        name = item.get('name')
        address = item.get('address')
        city = item.get('city')
        state = item.get('state')
        postal_code = item.get('postal_code')
        latitude = item.get('latitude')
        longitude = item.get('longitude')
        stars = item.get('stars')
        review_count = item.get('review_count')
        is_open = item.get('is_open')
        attributes = item.get('attributes')
        attributes_str = json.dumps(attributes)
        categories = item.get('categories')
        hours = item.get('hours')
        hours_str = json.dumps(hours)

        cursor.execute('''INSERT INTO yelp
                              (business_id, name, address, city, state, postal_code, latitude, longitude, stars,
                               review_count, is_open, attributes, categories, hours)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (business_id, name, address, city, state, postal_code, latitude, longitude, stars,
                        review_count, is_open, attributes_str, categories, hours_str))

    cursor.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS yelp_fts USING FTS5
                      (business_id, name, address, city, state, postal_code, latitude, longitude, stars,
                       review_count, is_open, attributes, categories, hours)''')

    cursor.execute('''INSERT INTO yelp_fts(business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, attributes, categories, hours)
                      SELECT business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, attributes, categories, hours FROM yelp''')

    print(f"Match time: {testMatch(cursor)}")







if __name__ == "__main__":
    main()