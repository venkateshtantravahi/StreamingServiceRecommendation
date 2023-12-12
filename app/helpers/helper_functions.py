from app.__init__ import redis_client
import random

def get_random_user_id_from_ratings():
    """
    Get a random user ID from the ratings sorted sets in Redis.

    Returns:
    str: A random user ID or None if no ratings are available.
    """
    # Get a list of keys for ratings
    rating_keys = list(redis_client.scan_iter(match='ratings:*'))
    
    # If there are no keys, return None
    if not rating_keys:
        return None
    
    # Randomly select a key
    random_key = random.choice(rating_keys)
    
    # Extract the user ID from the selected key
    random_user_id = random_key.decode().split(':')[1]
    
    return random_user_id

def get_top_rated_movies_for_user(user_id, num_movies=5):
    """
    Get the top-rated movies for a specific user.

    Args:
    user_id (str): The ID of the user whose top-rated movies are to be retrieved.
    num_movies (int): Number of top-rated movies to retrieve.

    Returns:
    list: A list of tuples containing movie titles and their ratings.
    """
    # Fetch the top-rated movies for the user
    top_ratings = redis_client.zrevrange(f"ratings:{user_id}", 0, num_movies-1, withscores=True)

    # Get movie titles for the top-rated movie IDs
    top_movie_details = []
    for movie_id_bytes, rating in top_ratings:
        movie_id = movie_id_bytes.decode('utf-8')
        movie_title = redis_client.hget(f"movie:{movie_id}", "title")
        if movie_title:
            top_movie_details.append((movie_title.decode('utf-8'), rating))

    return top_movie_details