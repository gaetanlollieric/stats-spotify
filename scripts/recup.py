import os
import requests
import time
from supabase import create_client
import base64

# --- CONFIG ---
SUPA_URL = os.environ.get("SUPABASE_URL")
SUPA_KEY = os.environ.get("SUPABASE_KEY")
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

supabase = create_client(SUPA_URL, SUPA_KEY)

def get_access_token():
    """Récupère un token temporaire (Client Credentials Flow)"""
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    res = requests.post("https://accounts.spotify.com/api/token", 
        data={"grant_type": "client_credentials"},
        headers={"Authorization": f"Basic {b64_auth}"})
    return res.json().get("access_token")

def main():
    print("🕵️  Recherche des titres incomplets (sans valence)...")
    
    # 1. On récupère TOUS les titres qui n'ont pas encore de 'valence'
    # On le fait par page de 1000 pour ne pas surcharger
    all_incomplete_tracks = []
    has_more = True
    page = 0
    
    while has_more:
        res = supabase.table("tracks").select("spotify_id")\
            .is_("valence", "null")\
            .range(page*1000, (page+1)*1000 - 1)\
            .execute()
        
        data = res.data
        if data:
            all_incomplete_tracks.extend(data)
            page += 1
            print(f"   ... {len(all_incomplete_tracks)} titres trouvés pour l'instant.")
        else:
            has_more = False

    total = len(all_incomplete_tracks)
    print(f"🎯 Total à mettre à jour : {total} titres.")
    
    if total == 0:
        print("✅ Tout est déjà à jour !")
        return

    token = get_access_token()
    
    # 2. Traitement par lots de 100 (limite Spotify)
    batch_size = 100
    track_ids = [t['spotify_id'] for t in all_incomplete_tracks]

    for i in range(0, total, batch_size):
        chunk = track_ids[i:i+batch_size]
        print(f"🔄 Traitement lot {i} à {i+len(chunk)}...")

        try:
            # Appel API Spotify
            url = f"https://api.spotify.com/v1/audio-features?ids={','.join(chunk)}"
            res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
            
            if res.status_code == 429:
                print("⏳ Rate limit ! Pause de 5 sec...")
                time.sleep(5)
                continue # On re-tentera au prochain run ou on saute ce chunk (simple ici)

            features = res.json().get("audio_features", [])
            
            # Préparation des updates
            updates = []
            for f in features:
                if f: # Spotify renvoie parfois null
                    updates.append({
                        "spotify_id": f["id"],
                        "valence": f["valence"],
                        "energy": f["energy"],
                        "danceability": f["danceability"],
                        "acousticness": f["acousticness"],
                        "instrumentalness": f["instrumentalness"]
                        # On ne touche pas au nom/artiste, juste les stats
                    })
            
            # Envoi vers Supabase
            if updates:
                supabase.table("tracks").upsert(updates).execute()
                
        except Exception as e:
            print(f"❌ Erreur sur ce lot : {e}")
            # On renouvelle le token au cas où
            token = get_access_token()

        time.sleep(0.5) # Politesse API

    print("✅ Terminé ! Actualise ton site web.")

if __name__ == "__main__":
    main()
