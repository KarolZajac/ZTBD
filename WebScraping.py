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
    cursor.execute("SELECT * FROM movies")
    rows = cursor.fetchall()
    for row in rows:
        print(f'ID: {row[0]}')
        print(f'Title: {row[1]}')
        print(f'Year: {row[2]}')
        print(f'Rating: {row[3]}')
        print('---')

def fullTextSearch(cursor):
    cursor.execute("CREATE VIRTUAL TABLE movies_fts USING FTS5(id, title, year, rating)")
    cursor.execute("INSERT INTO movies_fts(id, title, year, rating) SELECT id, title, year, rating FROM movies")
    search_query = "Alien"
    cursor.execute("SELECT * FROM movies_fts WHERE movies_fts MATCH ?", (search_query,))
    results = cursor.fetchall()
    cursor.execute("SELECT * FROM movies_fts WHERE nazwa LIKE ?", ('%' + search_query + '%',))

def main():
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS movies
                      (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      title TEXT,
                      year INTEGER,
                      rating REAL)''')
    scrapeData(cursor)
    fullTextSearch(cursor)
    # printDB(cursor)

if __name__ == "__main__":
    main()