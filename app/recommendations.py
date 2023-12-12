import redis
import json
import math
from collections import defaultdict
from app.__init__ import redis_client

# Function to get user ratings
def get_user_ratings(user_id):
    """
    Retrieve the ratings given by a specific user.

    Args:
    user_id (str): The ID of the user.

    Returns:
    dict: A dictionary of movie IDs and their corresponding ratings given by the user.
    """
    ratings = redis_client.zrange(f"ratings:{user_id}", 0, -1, withscores=True)
    return {movie_id.decode(): rating for movie_id, rating in ratings}


# Function to calculate similarity between two users
def calculate_similarity(user1_ratings, user2_ratings, similarity_cache):
    """
    Calculate similarity score between two users based on their ratings.

    Args:
    user1_ratings (dict): Ratings from user 1.
    user2_ratings (dict): Ratings from user 2.
    similarity_cache (dict): Cache to store computed similarity scores.

    Returns:
    float: The similarity score between the two users.
    """
    # Sort user IDs to create a consistent cache key
    # Extract and sort the movie IDs from each user's ratings
    user1_movies = sorted(user1_ratings.keys())
    user2_movies = sorted(user2_ratings.keys())
    
    # Create a consistent cache key based on sorted movie IDs
    cache_key = f"similarity:({user1_movies}):({user2_movies})"
    cached_similarity = similarity_cache.get(cache_key)
    
    if cached_similarity is not None:
        return cached_similarity

    common_movies = set(user1_ratings) & set(user2_ratings)
    num_ratings = len(common_movies)
    if num_ratings == 0:
        return 0

    user1_ratings_sum = sum([user1_ratings[movie] for movie in common_movies])
    user2_ratings_sum = sum([user2_ratings[movie] for movie in common_movies])
    user1_ratings_sq_sum = sum([pow(user1_ratings[movie], 2) for movie in common_movies])
    user2_ratings_sq_sum = sum([pow(user2_ratings[movie], 2) for movie in common_movies])
    product_sum = sum([user1_ratings[movie] * user2_ratings[movie] for movie in common_movies])

    numerator = product_sum - (user1_ratings_sum * user2_ratings_sum / num_ratings)
    denominator = math.sqrt((user1_ratings_sq_sum - pow(user1_ratings_sum, 2) / num_ratings) * 
                            (user2_ratings_sq_sum - pow(user2_ratings_sum, 2) / num_ratings))
    if denominator == 0:
        return 0

    similarity_score = numerator / denominator
    similarity_cache[cache_key] = similarity_score
    return similarity_score

# Function to get similar users
def get_similar_users(target_user_id, user_ids, similarity_cache):
    """
    Find users similar to a specified target user.

    Args:
    target_user_id (str): The target user's ID.
    user_ids (list): List of user IDs to compare with the target user.
    similarity_cache (dict): Cache to store computed similarity scores.

    Returns:
    list: A list of tuples containing similar user IDs and their similarity scores.
    """
    target_ratings = get_user_ratings(target_user_id)
    similar_users = []
    for user_id in user_ids:
        if user_id != f"ratings:{target_user_id}":
            other_user_ratings = get_user_ratings(user_id.decode('utf-8').split(":")[1]) 
            similarity = calculate_similarity(target_ratings, other_user_ratings, similarity_cache)
            similar_users.append((user_id, similarity))

    # Limiting the number of similar users for efficiency
    similar_users = sorted(similar_users, key=lambda x: x[1], reverse=True)[:10]
    return similar_users

#FUnction to retrun movie titles from movie_ids
def get_title_from_ids(movie_ids):
    # Fetch movie titles for the recommended movie IDs
    recommended_movie_titles = []
    for movie_id in movie_ids:
        movie_title = redis_client.hget(f"movie:{movie_id}", "title")
        if movie_title:
            recommended_movie_titles.append(movie_title.decode('utf-8'))
    return recommended_movie_titles

# Function to recommend movies
def recommend_movies(user_id, num_recommendations=5):
    """
    Recommend movies to a user based on the ratings of similar users.

    Args:
    user_id (str): The user ID for whom the recommendation is to be made.
    num_recommendations (int): The number of recommendations to generate.

    Returns:
    list: A list of movie IDs recommended for the user.
    """
    similarity_cache = {}  # Initialize an in-memory cache for similarity scores
    user_ids = redis_client.keys('ratings:*')
    similar_users = get_similar_users(user_id, user_ids, similarity_cache)
    movie_scores = defaultdict(float)

    for similar_user, similarity in similar_users:
        # Decode from bytes to string before splitting
        similar_user_id = similar_user.decode('utf-8').split(":")[1]
        # Ensure user_id is a string if necessary
        user_id_str = user_id if isinstance(user_id, str) else user_id.decode('utf-8')
        if similar_user_id != user_id_str:
            other_user_ratings = get_user_ratings(similar_user_id)
            for movie_id, rating in other_user_ratings.items():
                # Ensure movie_id is a string if necessary
                movie_id_str = movie_id if isinstance(movie_id, str) else movie_id.decode('utf-8')
                if movie_id_str not in get_user_ratings(user_id_str):
                    movie_scores[movie_id_str] += similarity * rating

    # Sort the movie scores and select the top recommendations
    sorted_scores = sorted(movie_scores.items(), key=lambda x: x[1], reverse=True)
    recommended_movie_ids = [movie for movie, _ in sorted_scores[:num_recommendations]]
    recommended_movie_titles = get_title_from_ids(recommended_movie_ids)
    return recommended_movie_titles

def add_user_rating(user_id, content_id, rating):
    """
    Add a user's rating for a movie and update the recommendations.

    Args:
    user_id (str): The ID of the user who is rating.
    content_id (str): The ID of the movie being rated.
    rating (float): The rating given by the user.
    """
    # Add or update the user's rating
    redis_client.zadd(f"ratings:{user_id}", {content_id: rating})

    # Trigger an update to the user's recommendations
    recommend_movies(user_id)
