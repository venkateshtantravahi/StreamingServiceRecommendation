from cryptography.fernet import Fernet
from . import redis_client
import os
import time
import json

# It's important to securely manage the encryption key in a production environment.
key = Fernet.generate_key()
cipher_suite = Fernet(key)

def create_cipher_suite():
    """
    Create and return a cipher suite for encryption and decryption.

    Returns:
    Fernet: A Fernet cipher suite.
    """
    return Fernet(key)

def encrypt_data(data):
    """
    Encrypt data using the Fernet cipher suite.

    Args:
    data (str): The data to be encrypted.

    Returns:
    str: Encrypted data, or None if an error occurs.
    """
    try:
        cipher_suite = create_cipher_suite()
        return cipher_suite.encrypt(data.encode()).decode()
    except Exception as e:
        print(f"Encryption error: {e}")
        return None

def decrypt_data(encrypted_data):
    """
    Decrypt data using the Fernet cipher suite.

    Args:
    encrypted_data (str): The encrypted data to be decrypted.

    Returns:
    str: Decrypted data, or None if an error occurs.
    """
    try:
        cipher_suite = create_cipher_suite()
        return cipher_suite.decrypt(encrypted_data.encode()).decode()
    except Exception as e:
        print(f"Decryption error: {e}")
        return None

def create_user(user_id, username, email, preferences, age):
    """
    Create a new user in the Redis database.

    Args:
    user_id (str): The ID of the user.
    username (str): The username of the user.
    email (str): The email of the user.
    preferences (str): The preferences of the user in JSON string format.
    age (int): The age of the user.
    """
    encrypted_email = encrypt_data(email)
    redis_client.hset(f"user:{user_id}", mapping={
        "username": username,
        "email": encrypted_email,
        "preferences": preferences,
        "age": age
    })

    # Initialize watch history and ratings for the user
    redis_client.rpush(f"watch_history:{user_id}", "")
    redis_client.zadd(f"ratings:{user_id}", {0: 0})  # Placeholder for ratings

def get_user(user_id):
    """
    Retrieve a user's data from the Redis database.

    Args:
    user_id (str): The ID of the user.

    Returns:
    dict: The user's data, or an empty dictionary if the user does not exist.
    """
    user_data = redis_client.hgetall(f"user:{user_id}")
    if user_data:
        user_data[b'email'] = decrypt_data(user_data[b'email'].decode())
    return user_data

def add_to_watch_history(user_id, content_id):
    """
    Add a movie to a user's watch history.

    Args:
    user_id (str): The ID of the user.
    content_id (str): The ID of the content being added to the watch history.
    """
    redis_client.rpush(f"watch_history:{user_id}", content_id)

def add_user_rating(user_id, content_id, rating):
    """
    Add or update a user's rating for a movie.

    Args:
    user_id (str): The ID of the user.
    content_id (str): The ID of the movie.
    rating (float): The rating given by the user.
    """
    redis_client.zadd(f"ratings:{user_id}", {content_id: rating})

# Movie-related Operations
def create_movie(content_id, title, director, cast, release_year, description, genres):
    """
    Create a new movie entry in the Redis database.

    Args:
    content_id (str): The ID of the movie.
    title (str): The title of the movie.
    director (str): The director of the movie.
    cast (list): A list of cast members.
    release_year (int): The release year of the movie.
    description (str): The description of the movie.
    genres (list): A list of genres associated with the movie.
    """
    movie_data = {
        "title": title,
        "director": director,
        "cast": ','.join(cast),
        "release_year": release_year,
        "description": description
    }
    redis_client.hmset(f"content:{content_id}", movie_data)
    for genre in genres:
        redis_client.sadd(f"genre:{genre}", content_id)

def get_movie(content_id):
    """
    Retrieve data for a specific movie.

    Args:
    content_id (str): The ID of the movie.

    Returns:
    dict: Data associated with the movie.
    """
    return redis_client.hgetall(f"content:{content_id}")

def add_movie_tags(content_id, tags):
    """
    Add tags to a movie.

    Args:
    content_id (str): The ID of the movie.
    tags (list): A list of tags to be added to the movie.
    """
    redis_client.sadd(f"movie_tags:{content_id}", *tags)

def get_movie_tags(content_id):
    """
    Retrieve tags associated with a specific movie.

    Args:
    content_id (str): The ID of the movie.

    Returns:
    set: A set of tags associated with the movie.
    """
    return redis_client.smembers(f"movie_tags:{content_id}")

def add_movie_tag_relevance(content_id, tag_relevance):
    """
    Add tag relevance scores to a movie.

    Args:
    content_id (str): The ID of the movie.
    tag_relevance (dict): A dictionary of tag IDs and their relevance scores.
    """
    redis_client.hmset(f"movie_tag_relevance:{content_id}", tag_relevance)

def get_movie_tag_relevance(content_id):
    """
    Retrieve tag relevance scores for a specific movie.

    Args:
    content_id (str): The ID of the movie.

    Returns:
    dict: Tag relevance scores for the movie.
    """
    return redis_client.hgetall(f"movie_tag_relevance:{content_id}")

def add_movie_links(content_id, imdb_id, tmdb_id):
    """
    Add external links (IMDb, TMDb) to a movie.

    Args:
    content_id (str): The ID of the movie.
    imdb_id (str): The IMDb ID of the movie.
    tmdb_id (str): The TMDb ID of the movie.
    """
    links = {"imdbId": imdb_id, "tmdbId": tmdb_id}
    redis_client.hmset(f"movie_links:{content_id}", links)

def get_movie_links(content_id):
    """
    Retrieve external links (IMDb, TMDb) for a specific movie.

    Args:
    content_id (str): The ID of the movie.

    Returns:
    dict: External links for the movie.
    """
    return redis_client.hgetall(f"movie_links:{content_id}")

# Genre-related Operations
def get_movies_by_genre(genre):
    """
    Retrieve movies associated with a specific genre.

    Args:
    genre (str): The genre to search for.

    Returns:
    set: A set of movie IDs associated with the genre.
    """
    return redis_client.smembers(f"genre:{genre}")

# User Interaction
def record_interaction(content_id, user_id):
    """
    Record a user's interaction with a movie.

    Args:
    content_id (str): The ID of the movie.
    user_id (str): The ID of the user.
    """
    timestamp = int(time.time())
    redis_client.zadd(f"interaction:{content_id}", {user_id: timestamp})

def get_user_interactions(content_id):
    """
    Retrieve all user interactions for a specific movie.

    Args:
    content_id (str): The ID of the movie.

    Returns:
    list: A list of tuples containing user IDs and their interaction timestamps.
    """
    return redis_client.zrange(f"interaction:{content_id}", 0, -1, withscores=True)

def get_all_users():
    """
    Retrieve a list of all user IDs. Checks the user table first; if empty, retrieves from the ratings table.

    Returns:
    list: A list of user IDs.
    """
    user_ids = []
    
    # First, try to get user IDs from the user table
    for key in redis_client.scan_iter("user:*"):
        user_id = key.decode().split(":")[1]
        user_ids.append(user_id)

    # If no user IDs in the user table, get them from the ratings table
    if not user_ids:
        for key in redis_client.scan_iter("ratings:*"):
            user_id = key.decode().split(":")[1]
            if user_id not in user_ids:
                user_ids.append(user_id)

    return user_ids
