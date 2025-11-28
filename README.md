# web-scraping-and-pyspark
!pip install requests beautifulsoup4 pandas


## *Scrape Book Data from First Page*

import requests
from bs4 import BeautifulSoup
import pandas as pd


# Target URL
url = "https://books.toscrape.com/"

# Send HTTP request
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Extract book containers
books = soup.select("article.product_pod")

# Lists to hold data
titles, prices, ratings, availability, genres = [], [], [], [], []

# Loop through each book
for book in books:
    title = book.h3.a['title']
    price = book.select_one(".price_color").text.strip()
    rating = book.p['class'][1]
    stock = book.select_one(".instock.availability").text.strip()

    titles.append(title)
    prices.append(price)
    ratings.append(rating)
    availability.append(stock)
    genres.append("Unknown")  # We’ll fix this in a later step

# Create DataFrame
df = pd.DataFrame({
    "Title": titles,
    "Price": prices,
    "Rating": ratings,
    "Availability": availability,
    "Genre": genres
})

df.head()


## Scrap all pages

import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

# Base URL
base_url = "https://books.toscrape.com/catalogue/page-{}.html"

# Initialize lists
titles, prices, ratings, availability, genres = [], [], [], [], []

# Loop through 1 to 50 pages
for page_num in range(1, 51):
    url = base_url.format(page_num)
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Page {page_num} not found, skipping.")
        continue

    soup = BeautifulSoup(response.text, 'html.parser')
    books = soup.select("article.product_pod")

    for book in books:
        title = book.h3.a['title']
        price = book.select_one(".price_color").text.strip()
        rating = book.p['class'][1]
        stock = book.select_one(".instock.availability").text.strip()

        # Visit the book's individual page to get genre
        book_url = urljoin(url, book.h3.a['href'])
        book_resp = requests.get(book_url)
        book_soup = BeautifulSoup(book_resp.text, 'html.parser')
        breadcrumb = book_soup.select('ul.breadcrumb li a')

        if len(breadcrumb) >= 3:
            genre = breadcrumb[2].text.strip()
        else:
            genre = "Unknown"

        # Append data
        titles.append(title)
        prices.append(price)
        ratings.append(rating)
        availability.append(stock)
        genres.append(genre)

    print(f"Page {page_num} scraped.")

# Create DataFrame
df_all = pd.DataFrame({
    "Title": titles,
    "Price": prices,
    "Rating": ratings,
    "Availability": availability,
    "Genre": genres
})

df_all.head()


df_all.to_csv("books_data.csv", index=False)
print("Data saved to books_data.csv")


## *Part2 - Pyspark*

!apt-get install openjdk-11-jdk -y
!pip install pyspark


import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("Books Data Analysis") \
    .getOrCreate()


from google.colab import files
uploaded = files.upload()


#spark dataframe
df_spark = spark.read.csv("books_data.csv", header=True, inferSchema=True)
df_spark.show(10)  # Show first 10 rows





df_spark.printSchema()


df_spark.show(10, truncate=False)


df_spark.describe().show()


#Filter Data

df_spark = df_spark.withColumn("Price", col("Price").substr(2, 10).cast("float"))  # Remove '£' and convert
df_spark.filter(col("Price") > 20).show(10, truncate=False)


#convert text ratings like "Three", "Five" to numbers:

from pyspark.sql.functions import when

rating_map = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5
}

# Map text to numbers
df_spark = df_spark.withColumn("Rating_Num",
    when(col("Rating") == "One", 1)
    .when(col("Rating") == "Two", 2)
    .when(col("Rating") == "Three", 3)
    .when(col("Rating") == "Four", 4)
    .when(col("Rating") == "Five", 5)
)

df_spark.filter(col("Rating_Num") >= 4).show(25, truncate=False)
