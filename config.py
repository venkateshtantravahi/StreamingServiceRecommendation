# config.py
import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'a-very-secret-key'
    REDIS_URL = "redis://localhost:6379/0"  # Change as per your Redis configuration