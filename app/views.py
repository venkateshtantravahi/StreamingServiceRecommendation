from flask import Flask, render_template, request, redirect, url_for
from flask_bootstrap import Bootstrap
from app.recommendations import recommend_movies, add_user_rating
from app.models import get_user, get_all_users
import random
from . import app, Bootstrap
from app.__init__ import redis_client
from app.helpers.helper_functions import get_random_user_id_from_ratings, get_top_rated_movies_for_user

@app.route('/')
def index():
    """
    The index route which shows user data and movie recommendations.

    Returns:
    Response: A rendered template with user data and recommendations or a 404 error.
    """
    user_id = get_random_user_id_from_ratings()
    if not user_id:
        return "No users available", 404

    try:
        user_data = get_user(user_id) if user_id else None
        # print(user_data)
        recommendations = recommend_movies(user_id) if user_id else []
        top_rated_movies = get_top_rated_movies_for_user(user_id) if user_id else []
    except Exception as e:
        # Log the exception and return an error message
        # Log format: print(f"Error in index route: {e}")
        print(f"Error in index route: {e}")
        return "An error occurred retrieving data", 500

    return render_template('index.html',user_data=user_data,  recommendations=recommendations,top_rated_movies=top_rated_movies, user_id=user_id) 


@app.route('/rate', methods=['POST'])
def rate_movie():
    """
    Route to handle rating of a movie by a user.

    Returns:
    Response: Redirects to the index route after processing the rating.
    """
    try:
        user_id = request.form['user_id']
        movie_id = request.form['movie_id']
        rating = int(request.form['rating'])
        add_user_rating(user_id, movie_id, rating)
    except Exception as e:
        # Log the exception and return an error message
        # Log format: print(f"Error in rate_movie route: {e}")
        return "An error occurred processing the rating", 500

    return redirect(url_for('index'))