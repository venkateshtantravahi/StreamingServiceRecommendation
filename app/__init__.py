# app/__init__.py
from flask import Flask
from redis import Redis
from config import Config
from flask_bootstrap import Bootstrap

app = Flask(__name__)
app.config.from_object(Config)

Bootstrap(app)

# Initialize Redis
redis_client = Redis.from_url(app.config['REDIS_URL'])

from app import views
