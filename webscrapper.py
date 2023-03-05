import time
import requests
from bs4 import BeautifulSoup
import json
import base64
import lzma
import hashlib
import pymongo
import urllib

# URL of the website to scrape
url = 'https://www.bestrandoms.com/random-movie-generator'

# Set the username and password
username = 'rikesh07'
password = 'Nomercy786@'

# Escape the username and password
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)

# Set up the MongoDB client

client = pymongo.MongoClient(f"mongodb://{escaped_username}:{escaped_password}@ac-rdienaa-shard-00-00.txwko2o.mongodb.net:27017,ac-rdienaa-shard-00-01.txwko2o.mongodb.net:27017,ac-rdienaa-shard-00-02.txwko2o.mongodb.net:27017/?ssl=true&replicaSet=atlas-11ccz3-shard-0&authSource=admin&retryWrites=true&w=majority")

db = client.movies
collection = db['movies']

# Batch size for MongoDB inserts
BATCH_SIZE = 200

def get_movie_id(title):
    # Generate a SHA256 hash of the movie title and return the first 8 characters as the movie ID
    hash_obj = hashlib.sha256(title.encode())
    return hash_obj.hexdigest()[:8]

def scrape_movies():
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the page using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract movie information
    movie_list = soup.find_all('li', class_='col-md-6')
    
    # Keep track of the number of movies inserted
    count = 0
    
    # Insert movies into MongoDB in batches
    movie_batch = []
    for movie in movie_list:
        # Extracting the image URL
        img_url = movie.find('img')['src']
        
        # Downloading the image data
        img_response = requests.get('https:' + img_url)
        img_data = img_response.content
        
        # Compressing the image data using LZMA
        compressed_img_data = lzma.compress(img_data)
        
        # Encoding the compressed image data in base64
        img_base64 = base64.b64encode(compressed_img_data).decode('utf-8')
        
        title = movie.find('span').text.strip()
        year = movie.find('span').text.split('(', 1)[1].strip(')')
        rating = movie.select_one('.grey span').text
        genre = movie.select_one('.grey span:nth-of-type(2)').text
        cast = movie.select_one('.cast b').text
        overview = movie.select_one('.detail')['data-overview'] if movie.select_one('.detail') else None
        
        # Generate a unique ID for the movie based on its title
        movie_id = get_movie_id(title)
        
        movie_dict = {
                'id': movie_id,
                'title': title,
                'rating': rating,
                'genre': genre,
                'cast': cast,
                'image': img_base64,
                'overview': overview,
                'year': year,
                'created_at': time.time()
            }
        
        movie_batch.append(movie_dict)
        count += 1
        
        # Insert batch of movies into MongoDB when batch size is reached
        if count == BATCH_SIZE:
            updates = []
            for movie in movie_batch:
                query = {'_id': movie['_id'], 'year': movie['year']}
                update = {'$replaceOne': {'filter': query, 'replacement': movie, 'upsert': True}}
                updates.append(update)
                collection.bulk_write(updates)
                print(f"{count} movies inserted into MongoDB")
                movie_batch = []
    

if __name__ == '__main__':
        while True:
            scrape_movies()
            # time.sleep(2) # Scrape movies every hour

