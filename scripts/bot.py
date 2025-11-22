import os
import requests
from supabase import create_client
import base64
import time

# Configuration
SUPA_URL = os.environ.get("SUPABASE_URL")
SUPA_KEY = os.environ.get("SUPABASE_KEY") # Doit être la clé SERVICE_ROLE (Secret)
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

# Connexion Supabase
supabase = create_client(SUPA_URL, SUPA_KEY)

def process_user(user):
    print(f"\n--- Traitement de : {user['display_name']} ({user['spotify_id']}) ---")
    
    # 1. Rafraîchir le token
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    
    try:
        token_res = requests.post("https://accounts.spotify.com/api/token", data={
            "grant_type": "refresh_token",
            "refresh_token": user['refresh_token']
        }, headers={"Authorization": f"Basic {b64_auth}"})
        
        token_data = token_res.json()
        
        if "error" in token_data:
            print(f"Erreur Refresh Token: {token_data}")
            # Optionnel : Supprimer l'user s'il a révoqué l'accès
            return

        access_token = token_data['access_token']
        
        # Si on reçoit un nouveau refresh token, on le met à jour
        if "refresh_token" in token_data:
            supabase.table("users").update({"refresh_token": token_data["refresh_token"]}).eq("spotify_id", user["spotify_id"]).execute()

        # 2. Récupérer l'historique
        recent_res = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50", 
            headers={"Authorization": f"Bearer {access_token}"})
        
        if recent_res.status_code != 200:
            print(f"Erreur API Spotify: {recent_res.status_code}")
            return

        tracks = recent_res.json().get("items", [])
        print(f"Récupéré {len(tracks)} titres.")

        # 3. Insérer dans l'historique
        count = 0
        to_insert = []
        for item in tracks:
            track = item["track"]
            to_insert.append({
                "played_at": item["played_at"],
                "track_name": track["name"],
                "artist_name": track["artists"][0]["name"],
                "album_name": track["album"]["name"],
                "spotify_id": track["id"],
                "duration_ms": track["duration_ms"],
                "user_id": user["spotify_id"] # On lie bien à CET utilisateur
            })

        if to_insert:
            # Upsert en masse
            res = supabase.table("spotify_history").upsert(to_insert, on_conflict="played_at").execute()
            print("Données insérées/mises à jour.")
            
        # Mettre à jour la date de dernière synchro
        supabase.table("users").update({"last_sync": "now()"}).eq("spotify_id", user["spotify_id"]).execute()

    except Exception as e:
        print(f"Erreur critique pour cet utilisateur: {e}")

# --- MAIN LOOP ---
# Récupérer tous les utilisateurs enregistrés
all_users = supabase.table("users").select("*").execute()

if all_users.data:
    print(f"Démarrage du bot pour {len(all_users.data)} utilisateurs...")
    for user in all_users.data:
        process_user(user)
        time.sleep(1) # Petite pause pour être gentil avec l'API
else:
    print("Aucun utilisateur trouvé dans la table 'users'.")
