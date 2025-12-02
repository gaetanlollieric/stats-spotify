import os
import requests
from supabase import create_client
import base64
import time

# --- CONFIGURATION ---
SUPA_URL = os.environ.get("SUPABASE_URL")
SUPA_KEY = os.environ.get("SUPABASE_KEY")
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")

# Initialisation Supabase
supabase = create_client(SUPA_URL, SUPA_KEY)

# Stats globales pour le message Discord final
stats_discord = {
    "users_processed": [],
    "total_tracks": 0
}

def get_spotify_token(refresh_token):
    """Récupère un access_token frais via le refresh_token"""
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    try:
        res = requests.post("https://accounts.spotify.com/api/token", data={
            "grant_type": "refresh_token", 
            "refresh_token": refresh_token
        }, headers={"Authorization": f"Basic {b64_auth}"})
        return res.json()
    except Exception as e:
        print(f"Erreur Token: {e}")
        return None

def process_user(user):
    print(f"\n--- Traitement de : {user['display_name']} ---")
    
    # 1. Authentification
    token_data = get_spotify_token(user['refresh_token'])
    if not token_data or "error" in token_data:
        print("❌ Erreur d'authentification Spotify.")
        return     
    
    access_token = token_data['access_token']
    
    # Mise à jour du token si changé
    if "refresh_token" in token_data:
        supabase.table("users").update({
            "refresh_token": token_data["refresh_token"]
        }).eq("spotify_id", user["spotify_id"]).execute()

    # 2. Récupération Historique (50 derniers titres)
    recent_res = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50", 
        headers={"Authorization": f"Bearer {access_token}"}
    )
    
    if recent_res.status_code != 200:
        print(f"❌ Erreur API Spotify (History): {recent_res.status_code}")
        return

    tracks_data = recent_res.json().get("items", [])
    if not tracks_data:
        print("⚠️ Aucun historique renvoyé par Spotify.")
        return

    # 3. Préparation des données
    artists_db_data = []
    tracks_db_data = []
    history_db_data = []

    # Récupération des IDs uniques
    artist_ids = list(set([t["track"]["artists"][0]["id"] for t in tracks_data if t["track"]]))
    
    # --- 3a. Récupération des Genres (Artistes) ---
    for i in range(0, len(artist_ids), 50):
        chunk = artist_ids[i:i+50]
        try:
            art_res = requests.get(
                f"https://api.spotify.com/v1/artists?ids={','.join(chunk)}",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            if art_res.status_code == 200:
                for a in art_res.json().get("artists", []):
                    artists_db_data.append({
                        "spotify_id": a["id"], 
                        "name": a["name"], 
                        "genres": a["genres"]
                    })
        except Exception as e:
            print(f"⚠️ Erreur récup Artistes: {e}")

    # --- Construction des listes pour la BDD ---
    for item in tracks_data:
        if not item.get("track"): continue
        
        track = item["track"]
        tid = track["id"]
        clean_date = item["played_at"].replace('Z', '+00:00')
        
        tracks_db_data.append({
            "spotify_id": tid, 
            "name": track["name"],
            "artist_id": track["artists"][0]["id"], 
            "album_name": track["album"]["name"],
            "duration_ms": track["duration_ms"],
            "popularity": track.get("popularity", 0) # On garde ça car c'est dispo et cool
        })
        
        history_db_data.append({
            "played_at": clean_date,
            "user_id": user["spotify_id"],
            "track_id": tid
        })

    # 4. ENVOI EN BASE DE DONNÉES
    
    if artists_db_data:
        try: supabase.table("artists").upsert(artists_db_data).execute()
        except Exception as e: print(f"⚠️ Erreur insert Artistes: {e}")
    
    if tracks_db_data:
        unique_tracks = {v['spotify_id']:v for v in tracks_db_data}.values()
        try: supabase.table("tracks").upsert(list(unique_tracks)).execute()
        except Exception as e: print(f"⚠️ Erreur insert Tracks: {e}")

    if history_db_data:
        try:
            response = supabase.table("listening_history").upsert(
                history_db_data, 
                on_conflict="played_at, user_id", 
                ignore_duplicates=True
            ).execute()
            
            nb_reels_ajouts = len(response.data)
            if nb_reels_ajouts > 0:
                print(f"✅ {nb_reels_ajouts} nouveaux titres sauvegardés.")
                stats_discord["total_tracks"] += nb_reels_ajouts
                stats_discord["users_processed"].append({
                    "name": user["display_name"],
                    "count": nb_reels_ajouts
                })
            else:
                print("💤 Rien de nouveau.")
        except Exception as e:
            print(f"⚠️ Erreur insert History: {e}")

    supabase.table("users").update({"last_sync": "now()"}).eq("spotify_id", user["spotify_id"]).execute()

# --- MAIN ---
def main():
    print("🚀 Démarrage du script (Version Clean)...")
    try:
        all_users = supabase.table("users").select("*").execute()
    except Exception as e:
        print(f"❌ Erreur connexion BDD: {e}")
        return

    if all_users.data:
        for user in all_users.data:
            try: process_user(user)
            except Exception as e: print(f"❌ Crash user {user.get('display_name')}: {e}")
            time.sleep(1)

        if DISCORD_WEBHOOK and stats_discord["total_tracks"] > 0:
            msg = f"🎵 **Sync OK** (+{stats_discord['total_tracks']} titres)"
            try: requests.post(DISCORD_WEBHOOK, json={"content": msg})
            except: pass
    else:
        print("⚠️ Aucun utilisateur.")

if __name__ == "__main__":
    main()
