from flask import Flask, jsonify
import requests
import json
import os

app = Flask(__name__)

# Configuration (use environment variables in production)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

# Load friends data from local JSON
def load_friends_data():
    try:
        with open('friends_data.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"friends": []}

# Function to get Spotify access token (if needed for API data)
def get_spotify_token():
    # Implement token refresh if necessary
    pass

@app.route('/api/streams/<track_id>')
def get_streams_data(track_id):
    # For simplicity, return mock data or integrate with Supabase
    # In real implementation, fetch from Supabase and merge with friends

    # Example: Return combined data
    personal_data = [
        {"date": "2023-01-01", "plays": 2},
        {"date": "2023-01-02", "plays": 1},
        {"date": "2023-01-03", "plays": 3},
    ]

    friends_data = load_friends_data()["friends"]

    # Merge and aggregate
    data = {"personal": personal_data, "friends": friends_data}
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)