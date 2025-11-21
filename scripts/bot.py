import os
import requests
from supabase import create_client
import base64

# 1. Configuration
SUPA_URL = os.environ.get("SUPABASE_URL")
SUPA_KEY = os.environ.get("SUPABASE_KEY")
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("SPOTIFY_REFRESH_TOKEN")

supabase = create_client(SUPA_URL, SUPA_KEY)

# 2. Obtenir un nouveau Access Token frais
auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
b64_auth = base64.b64encode(auth_str.encode()).decode()

token_res = requests.post("https://accounts.spotify.com/api/token", data={
    "grant_type": "refresh_token",
    "refresh_token": REFRESH_TOKEN
}, headers={"Authorization": f"Basic {b64_auth}"})

access_token = token_res.json().get("access_token")

if not access_token:
    print("Erreur Token:", token_res.json())
    exit(1)

# 3. Récupérer les 50 derniers titres
recent_res = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50", 
    headers={"Authorization": f"Bearer {access_token}"})
tracks = recent_res.json().get("items", [])

print(f"Récupéré {len(tracks)} titres de Spotify.")

# 4. Insérer dans Supabase
count = 0
for item in tracks:
    track = item["track"]
    played_at = item["played_at"]
    
    data = {
        "played_at": played_at,
        "track_name": track["name"],
        "artist_name": track["artists"][0]["name"],
        "album_name": track["album"]["name"],
        "spotify_id": track["id"],
        "duration_ms": track["duration_ms"]
    }
    
    # Upsert (ignore si played_at existe déjà grâce à la contrainte UNIQUE SQL)
    try:
        supabase.table("spotify_history").upsert(data, on_conflict="played_at").execute()
        count += 1
    except Exception as e:
        print(f"Erreur ou doublon: {e}")

print(f"Traitement terminé. {count} tentatives d'insertion.")
