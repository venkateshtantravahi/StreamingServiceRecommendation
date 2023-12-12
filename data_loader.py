import csv, os, json
from redis.exceptions import RedisError
from app.models import create_user, add_to_watch_history, add_user_rating, create_movie, add_movie_tags, add_movie_tag_relevance, add_movie_links, record_interaction
from app.__init__ import redis_client

movies_file_path = 'app/static/ml-25m/movies.csv'
ratings_file_path = 'app/static/ml-25m/ratings.csv'
tags_file_path = 'app/static/ml-25m/tags.csv'
links_file_path = 'app/static/ml-25m/links.csv'
genome_tags_file_path = 'app/static/ml-25m/genome-tags.csv'
genome_scores_file_path = 'app/static/ml-25m/genome-scores.csv'

# Function to load movies using Redis pipeline
def load_movies(filepath):
    pipeline = redis_client.pipeline()
    batch_size = 1000
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for count, row in enumerate(reader, start=1):
                movie_id = row.get('movieId')
                title = row.get('title', 'Unknown Title')
                genres = json.dumps(row.get('genres', 'Unknown').split('|'))
                pipeline.hmset(f"movie:{movie_id}", {'title': title, 'genres': genres})
                if count % batch_size == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
            pipeline.execute()
    except (csv.Error, RedisError) as e:
        print(f"Error loading movies: {e}")

# Load Ratings with batch processing
def load_ratings(filepath):
    pipeline = redis_client.pipeline()
    batch_size = 1000
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for count, row in enumerate(reader, start=1):
                pipeline.zadd(f"ratings:{row['userId']}", {row['movieId']: float(row['rating'])}) #zadd for sorted set of ratings
                if count % batch_size == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
            pipeline.execute()
    except (csv.Error, RedisError) as e:
        print(f"Error loading ratings: {e}")

# Load Links with batch processing
def load_links(filepath):
    pipeline = redis_client.pipeline()
    batch_size = 1000
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for count, row in enumerate(reader, start=1):
                # Use Redis HMSET command to store links
                pipeline.hmset(f"link:{row['movieId']}", {'imdbId': row['imdbId'], 'tmdbId': row['tmdbId']})
                if count % batch_size == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
            pipeline.execute()
    except (csv.Error, RedisError) as e:
        print(f"Error loading links: {e}")

# Load Tags with batch processing
def load_tags(filepath):
    pipeline = redis_client.pipeline()
    batch_size = 1000
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for count, row in enumerate(reader, start=1):
                # Use Redis SADD command to add tags to a set
                pipeline.sadd(f"tags:{row['movieId']}", row['tag'])
                if count % batch_size == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
            pipeline.execute()
    except (csv.Error, RedisError) as e:
        print(f"Error loading tags: {e}")

# Load Genome Tags with batch processing
def load_genome_tags(filepath):
    pipeline = redis_client.pipeline()
    batch_size = 1000
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for count, row in enumerate(reader, start=1):
                # Storing genome score as a hash
                pipeline.set(f"genome_tag:{row['tagId']}", row['tag'])
                if count % batch_size == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
            pipeline.execute()
    except (csv.Error, RedisError) as e:
        print(f"Error loading genome tags: {e}")

# Load Genome Scores with batch processing
def load_genome_scores(filepath):
    pipeline = redis_client.pipeline()
    batch_size = 1000
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for count, row in enumerate(reader, start=1):
                # Storing genome score as a hash
                pipeline.hmset(f"genome_score:{row['movieId']}", {row['tagId']: row['relevance']})
                if count % batch_size == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
            pipeline.execute()
    except (csv.Error, RedisError) as e:
        print(f"Error loading genome scores: {e}")


print("Loading movies...")
load_movies(movies_file_path)

print("Loading ratings...")
load_ratings(ratings_file_path)

print("Loading links...")
load_links(links_file_path)

print("Loading tags...")
load_tags(tags_file_path)

print("Loading genome-tags...")
load_genome_tags(genome_tags_file_path)

print("Loading genome_scores...")
load_genome_scores(genome_scores_file_path)


print("Data loading complete.")