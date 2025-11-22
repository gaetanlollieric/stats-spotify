import os
import requests
from supabase import create_client
import base64
import time

# Configuration
SUPA_URL = os.environ.get("SUPABASE_URL")
SUPA_KEY = os.environ.get("SUPABASE_KEY")
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")  # NOUVEAU

supabase = create_client(SUPA_URL, SUPA_KEY)

# Stats globales
total_tracks_added = 0
users_processed = []

def process_user(user):
    global total_tracks_added
    print(f"\n--- Traitement de : {user['display_name']} ({user['spotify_id']}) ---")
    
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
            return

        access_token = token_data['access_token']
        
        if "refresh_token" in token_data:
            supabase.table("users").update({"refresh_token": token_data["refresh_token"]}).eq("spotify_id", user["spotify_id"]).execute()

        recent_res = requests.get(
            "https://api.spotify.com/v1/me/player/recently-played?limit=50", 
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if recent_res.status_code != 200:
            print(f"Erreur API Spotify: {recent_res.status_code}")
            return

        tracks = recent_res.json().get("items", [])
        print(f"Récupéré {len(tracks)} titres.")

        # Comptage des nouveaux titres
        played_at_list = [item["played_at"] for item in tracks]
        existing = supabase.table("spotify_history").select("played_at").in_("played_at", played_at_list).eq("user_id", user["spotify_id"]).execute()
        already_in_db = {item["played_at"] for item in existing.data}

        new_tracks = [item for item in tracks if item["played_at"] not in already_in_db]
        
        to_insert = []
        for item in new_tracks:
            track = item["track"]
            to_insert.append({
                "played_at": item["played_at"],
                "track_name": track["name"],
                "artist_name": track["artists"][0]["name"],
                "album_name": track["album"]["name"],
                "spotify_id": track["id"],
                "duration_ms": track["duration_ms"],
                "user_id": user["spotify_id"]
            })

        if to_insert:
            supabase.table("spotify_history").upsert(to_insert, on_conflict="played_at").execute()

        # On affiche le compte réel de nouveaux titres
        print(f"{len(to_insert)} titres ajoutés (nouveaux).")
        total_tracks_added += len(to_insert)

        users_processed.append({
            "name": user["display_name"],
            "tracks": len(to_insert)
        })
        
        supabase.table("users").update({"last_sync": "now()"}).eq("spotify_id", user["spotify_id"]).execute()

    except Exception as e:
        print(f"Erreur critique pour cet utilisateur: {e}")


# --- MAIN ---
all_users = supabase.table("users").select("*").execute()

if all_users.data:
    print(f"Démarrage du bot pour {len(all_users.data)} utilisateurs...")
    for user in all_users.data:
        process_user(user)
        time.sleep(1)
    
    # ENVOI DE LA NOTIFICATION DISCORD
    if DISCORD_WEBHOOK:
        message = f"🎵 **Mise à jour Spotify terminée !**\n\n"
        message += f"**Utilisateurs traités :** {len(users_processed)}\n"
        message += f"**Total de nouveaux titres :** {total_tracks_added}\n\n"
        
        for u in users_processed:
            message += f"• {u['name']} : {u['tracks']} titre(s)\n"
        
        discord_payload = {"content": message}
        requests.post(DISCORD_WEBHOOK, json=discord_payload)
        print("Notification Discord envoyée !")
else:
    print("Aucun utilisateur trouvé dans la table 'users'.")
