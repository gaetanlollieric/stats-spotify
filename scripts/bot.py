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
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")

supabase = create_client(SUPA_URL, SUPA_KEY)

# Stats globales pour Discord
stats_discord = {
    "users_processed": [],
    "total_tracks": 0
}

def normalize_played_at(dt):
    """Normalise le format de date pour correspondre strictement entre Spotify et Supabase"""
    if not dt: return ""
    # On remplace le Z par +00:00 pour le standard ISO strict
    return dt.replace('Z', '+00:00')

def get_spotify_token(refresh_token):
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    try:
        res = requests.post("https://accounts.spotify.com/api/token", data={
            "grant_type": "refresh_token", "refresh_token": refresh_token
        }, headers={"Authorization": f"Basic {b64_auth}"})
        return res.json()
    except:
        return None

def process_user(user):
    print(f"\n--- Traitement de : {user['display_name']} ---")
    
    # 1. Auth
    token_data = get_spotify_token(user['refresh_token'])
    if not token_data or "error" in token_data:
        print("Erreur Token")
        return
        
    access_token = token_data['access_token']
    
    # Mise à jour du refresh token si nouveau
    if "refresh_token" in token_data:
        supabase.table("users").update({"refresh_token": token_data["refresh_token"]}).eq("spotify_id", user["spotify_id"]).execute()

    # 2. Récupération Historique
    recent_res = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50", 
                              headers={"Authorization": f"Bearer {access_token}"})
    
    if recent_res.status_code != 200: return
    tracks_data = recent_res.json().get("items", [])
    if not tracks_data: return

    # 3. Filtrer ce qui est DÉJÀ en base pour ce user
    played_at_list = [normalize_played_at(item["played_at"]) for item in tracks_data]
    
    # On récupère ce qui existe déjà dans la nouvelle table
    existing = supabase.table("listening_history").select("played_at").in_("played_at", played_at_list).eq("user_id", user["spotify_id"]).execute()
    
    # On normalise aussi ce qui vient de la base pour être sûr de la comparaison
    already_in_db = {normalize_played_at(item["played_at"]) for item in existing.data}

    new_items = [t for t in tracks_data if normalize_played_at(t["played_at"]) not in already_in_db]
    
    if not new_items:
        print("Aucun nouveau titre.")
        return

    print(f"{len(new_items)} nouveaux titres à traiter.")

    # 4. Récupération des Genres (Infos Artistes)
    artist_ids = list(set([t["track"]["artists"][0]["id"] for t in new_items]))
    
    artists_db_data = []
    # Spotify limite à 50 IDs par appel
    for i in range(0, len(artist_ids), 50):
        chunk = artist_ids[i:i+50]
        art_res = requests.get(f"https://api.spotify.com/v1/artists?ids={','.join(chunk)}",
                               headers={"Authorization": f"Bearer {access_token}"})
        if art_res.status_code == 200:
            for a in art_res.json().get("artists", []):
                artists_db_data.append({
                    "spotify_id": a["id"],
                    "name": a["name"],
                    "genres": a["genres"]
                })

    # 5. INSERTIONS 
    
    # A. Artistes
    if artists_db_data:
        supabase.table("artists").upsert(artists_db_data).execute()
    
    # B. Titres
    tracks_db_data = []
    for item in new_items:
        track = item["track"]
        tracks_db_data.append({
            "spotify_id": track["id"],
            "name": track["name"],
            "artist_id": track["artists"][0]["id"],
            "album_name": track["album"]["name"],
            "duration_ms": track["duration_ms"]
        })
    
    unique_tracks = {v['spotify_id']:v for v in tracks_db_data}.values()
    if unique_tracks:
        supabase.table("tracks").upsert(list(unique_tracks)).execute()

    # C. Historique (CORRIGÉ ICI)
    history_db_data = []
    for item in new_items:
        history_db_data.append({
            "played_at": normalize_played_at(item["played_at"]),
            "user_id": user["spotify_id"],
            "track_id": item["track"]["id"]
        })

    if history_db_data:
        # AJOUT DE ignore_duplicates=True POUR ÉVITER L'ERREUR 23505
        supabase.table("listening_history").upsert(
            history_db_data, 
            on_conflict="played_at, user_id", 
            ignore_duplicates=True
        ).execute()
        
        stats_discord["total_tracks"] += len(history_db_data)
        stats_discord["users_processed"].append({
            "name": user["display_name"],
            "count": len(history_db_data)
        })

    # Update Last Sync
    supabase.table("users").update({"last_sync": "now()"}).eq("spotify_id", user["spotify_id"]).execute()


# --- MAIN ---
all_users = supabase.table("users").select("*").execute()

if all_users.data:
    for user in all_users.data:
        try:
            process_user(user)
        except Exception as e:
            print(f"Erreur user {user['display_name']}: {e}")
        time.sleep(1)

    # Notification Discord
    if DISCORD_WEBHOOK and stats_discord["users_processed"]:
        msg = f"🎵 **Mise à jour terminée !**\nTotal: {stats_discord['total_tracks']} nouveaux titres.\n"
        for u in stats_discord["users_processed"]:
            msg += f"- {u['name']} : {u['count']}\n"
        requests.post(DISCORD_WEBHOOK, json={"content": msg})
