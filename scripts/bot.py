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

def normalize_played_at(dt):
    # Transforme le Z de fin en +00:00 si présent
    return dt.replace('Z', '+00:00')


# ... (Début du script identique : imports, config, normalize_played_at ...)

def process_user(user):
    global total_tracks_added
    print(f"\n--- Traitement de : {user['display_name']} ---")
    
    # ... (Authentification OAuth identique jusqu'à access_token) ...
    # ... (Récupération token ... identique) ...
    
    # 1. Récupérer l'historique
    recent_res = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50", 
        headers={"Authorization": f"Bearer {access_token}"}
    )
    tracks_data = recent_res.json().get("items", [])
    
    if not tracks_data:
        return

    # 2. Récupérer les genres (Il faut demander les infos Artistes à part)
    artist_ids = list(set([t["track"]["artists"][0]["id"] for t in tracks_data])) # Liste unique des IDs
    
    # On ne peut demander que 50 artistes max d'un coup, on découpe si besoin
    artists_info = {}
    for i in range(0, len(artist_ids), 50):
        chunk = artist_ids[i:i+50]
        ids_str = ",".join(chunk)
        art_res = requests.get(
            f"https://api.spotify.com/v1/artists?ids={ids_str}",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if art_res.status_code == 200:
            for a in art_res.json().get("artists", []):
                artists_info[a["id"]] = {
                    "name": a["name"],
                    "genres": a["genres"] # VOICI LES GENRES !
                }

    # 3. Préparer les données pour les 3 tables
    new_history = []
    artists_to_upsert = []
    tracks_to_upsert = []

    # On vérifie ce qu'on a déjà en base pour éviter les doublons d'historique
    played_at_list = [normalize_played_at(item["played_at"]) for item in tracks_data]
    existing = supabase.table("listening_history").select("played_at").in_("played_at", played_at_list).eq("user_id", user["spotify_id"]).execute()
    already_in_db = {normalize_played_at(item["played_at"]) for item in existing.data}

    for item in tracks_data:
        p_at = normalize_played_at(item["played_at"])
        if p_at in already_in_db:
            continue # On saute si déjà enregistré

        track = item["track"]
        artist = track["artists"][0]
        a_id = artist["id"]
        t_id = track["id"]

        # Info Artiste (avec Genre)
        if a_id in artists_info:
            artists_to_upsert.append({
                "spotify_id": a_id,
                "name": artists_info[a_id]["name"],
                "genres": artists_info[a_id]["genres"]
            })

        # Info Titre
        tracks_to_upsert.append({
            "spotify_id": t_id,
            "name": track["name"],
            "artist_id": a_id,
            "album_name": track["album"]["name"],
            "duration_ms": track["duration_ms"]
        })

        # Info Historique (Léger !)
        new_history.append({
            "played_at": p_at,
            "user_id": user["spotify_id"],
            "track_id": t_id
        })

    # 4. Envoyer tout ça à Supabase (Ordre important !)
    
    # A. Les Artistes (upsert pour mettre à jour si existe déjà)
    if artists_to_upsert:
        # Astuce: deduplicate list of dicts
        unique_artists = {v['spotify_id']:v for v in artists_to_upsert}.values()
        supabase.table("artists").upsert(list(unique_artists)).execute()

    # B. Les Titres
    if tracks_to_upsert:
        unique_tracks = {v['spotify_id']:v for v in tracks_to_upsert}.values()
        supabase.table("tracks").upsert(list(unique_tracks)).execute()

    # C. L'Historique
    if new_history:
        supabase.table("listening_history").upsert(new_history).execute()
        print(f"{len(new_history)} nouveaux titres ajoutés (Clean DB).")
        total_tracks_added += len(new_history)
        
        users_processed.append({
            "name": user["display_name"],
            "tracks": len(new_history)
        })
        
        # Mise à jour timestamp user
        supabase.table("users").update({"last_sync": "now()"}).eq("spotify_id", user["spotify_id"]).execute()




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
