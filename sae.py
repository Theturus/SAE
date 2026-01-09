"""
Notre script du programme principal de l'application de recherche de restaurants :
"""

from pymongo import MongoClient
from geopy.distance import geodesic
import psycopg2
from psycopg2.extras import RealDictCursor, register_hstore
import json
import time
import heapq
from difflib import get_close_matches
from collections import defaultdict


# --- Paramètres de connexion aux serveurs PostgreSQL et MongoDB ---

# Configuration PostgreSQL
# Serveur PostgreSQL utilisé pour le cache des résultats et pour la sauvegarde des résultats.
PG_HOST = "localhost"
PG_DATABASE = "sae"
PG_USER = "postgres"

# Pour la sécurité, nous avons créé un fichier variables.py pour stocker 
# le mot de passe de la base de données PostgreSQL. Vu que vous n'avez pas accès à ce fichier,
# nous avons prévu l'alternative où vous pouvez utiliser votre mot de passe sans créer le fichier variables.py, 
# vous pouvez le modifier dans le code en remplacant la ligne PG_PASSWORD = "" par votre mot de passe.

try:
    import variables
    PG_PASSWORD = variables.mdp
except ImportError:
    PG_PASSWORD = ""

# Configuration MongoDB
# Connexion à la base de données MongoDB
client = MongoClient("mongodb://localhost:27017/")
database = client["sae_mongo"]
collection = database["restaurants"]

# Connexion à la base de données PostgreSQL
pg_conn = None
try:
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    pg_conn.autocommit = True # Autocommit pour que les transactions soient automatiquement validées.
    pg_cursor = pg_conn.cursor()
    
    # Activation de l'extension hstore si elle n'existe pas
    pg_cursor.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
    register_hstore(pg_conn)
    
    # Création de la table rhistory pour le cache
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS rhistory (
            id SERIAL PRIMARY KEY,
            latitude NUMERIC(10, 6) NOT NULL,
            longitude NUMERIC(10, 6) NOT NULL,
            cuisine VARCHAR(255),
            results HSTORE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Création d'un index composite pour les recherches de cache pour optimiser les performances de la recherche des restaurants.
    pg_cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_rhistory_coords_cuisine 
        ON rhistory(latitude, longitude, cuisine);
    """)
    
    # Index sur created_at pour accélérer le tri par date (ORDER BY created_at DESC)
    # Dans le cas où les résultats sont identiques, on trie par date de création la plus récente pour optimiser les performances de la recherche.
    pg_cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_rhistory_created 
        ON rhistory(created_at DESC);
    """)
    
    # Analyse de la table pour mettre à jour les statistiques utilisées par le planificateur pour optimiser les performances de la recherche des restaurants.
    pg_cursor.execute("ANALYZE rhistory;")
    
    pg_cursor.close()
    print("Connexion reussie a PostgreSQL")
except Exception as e:
    print(f"Erreur de connexion PostgreSQL : {e}")
    pg_conn = None

# Connexion à MongoDB pour la recherche des restaurants.
# Si la connexion à MongoDB échoue, le programme affiche un message d'erreur et ne peut pas rechercher les restaurants.
mongo_connected = False
try:
    client.admin.command('ping') # Ping pour vérifier si la connexion à MongoDB est active
    print("Connexion reussie a MongoDB")
    mongo_connected = True
except Exception as e: # Erreur de connexion à MongoDB
    print('Erreur de connexion MongoDB')
    mongo_connected = False


# --- Position de l'utilisateur ---

# Définition des plages de validation des coordonnées de l'utilisateur
LAT_MIN = 40.50
LAT_MAX = 41.20
LON_MIN = -74.26
LON_MAX = -73.20

# Fonction pour valider et récupérer les coordonnées de l'utilisateur
def validate_and_get_coordinates():

    print("\n" + "="*60)
    print("SAISIE DES COORDONNÉES")
    print("="*60)
    print(f"Plage valide : Latitude [{LAT_MIN}, {LAT_MAX}], Longitude [{LON_MIN}, {LON_MAX}]")
    print("Exemple : Latitude 40.7589, Longitude -73.9851 (Times Square)\n")
    
    # Saisie et Validation de la Latitude
    user_lat = None
    while True:
        try:
            user_lat_str = input(f"Latitude (entre {LAT_MIN} et {LAT_MAX}) : ").strip()
            
            # On vérifie si la latitude est vide
            if not user_lat_str:
                print("Erreur : La latitude ne peut pas être vide. Veuillez réessayer.")
                continue
            
            user_lat = float(user_lat_str)
            
            # On vérifie si la latitude est dans la plage valide
            if LAT_MIN <= user_lat <= LAT_MAX:
                break
            else:
                print(f"Erreur de Valeur: La latitude {user_lat} est hors de la plage valide [{LAT_MIN}, {LAT_MAX}]")
                
        except ValueError:
            print("Erreur de Format: La latitude doit être un nombre (ex: 40.7589)")
        except KeyboardInterrupt:
            print("\n\nOperation annulee par l'utilisateur")
            exit(0) # On quitte le programme si l'utilisateur interrompt la saisie
    
    # Saisie et Validation de la Longitude
    user_lon = None
    while True:
        try:
            user_lon_str = input(f"Longitude (entre {LON_MIN} et {LON_MAX}) : ").strip()
            
            # On vérifie si la longitude est vide
            if not user_lon_str:
                print("Erreur : La longitude ne peut pas être vide. Veuillez réessayer.")
                continue
            
            user_lon = float(user_lon_str)
            
            # On vérifie si la longitude est dans la plage valide
            if LON_MIN <= user_lon <= LON_MAX:
                break
            else:
                print(f"Erreur de Valeur: La longitude {user_lon} est hors de la plage valide [{LON_MIN}, {LON_MAX}]")
                
        except ValueError:
            print("Erreur de Format: La longitude doit etre un nombre (ex: -73.9851)")
        except KeyboardInterrupt:
            print("\n\nOperation annulee par l'utilisateur")
            exit(0) # On quitte le programme si l'utilisateur interrompt la saisie ici également
    
    user_position = (user_lat, user_lon) # Création du tuple contenant la latitude et la longitude 
    print(f"\nCoordonnees validees : ({user_lat}, {user_lon})")
    return user_position, user_lat, user_lon

# Appel de la fonction
user_position, user_lat, user_lon = validate_and_get_coordinates()


# --- Filtre de cuisine ---

# Au lieu de renvoyer tous les restaurants, cette fonction va permettre de trouver des suggestions de cuisine proches
# si l'utilisateur renseigne une cuisine invalide ou une faute de frappe.
# Pour cela, on va utiliser la fonction get_close_matches de difflib.

def get_cuisine_suggestions(user_input, available_cuisines):
    # On vérifie si l'utilisateur a saisi une cuisine ou si la liste des cuisines disponibles est vide.
    if not user_input or not available_cuisines:
        # Si c'est le cas, on retourne une liste vide.
        return []
    
    # On trouve les correspondances proches (ratio de similarité > 0.6) 
    matches = get_close_matches(user_input, available_cuisines, n=3, cutoff=0.6)
    return matches


# Fonction pour valider et récupérer la cuisine de l'utilisateur avec suggestions en cas de faute de frappe.
def validate_and_get_cuisine():

    print("\n" + "="*60)
    print("FILTRE DE CUISINE")
    print("="*60)
    
    # Récupération de la liste des cuisines disponibles dans la base de données MongoDB.
    available_cuisines = []
    if mongo_connected:
        try:
            distinct_cuisines = [c for c in collection.distinct('cuisine') if c]
            available_cuisines = [c.lower() for c in distinct_cuisines] # Conversion en minuscules pour la comparaison.
            print(f"{len(available_cuisines)} types de cuisine disponibles")
            print(f"Exemples : {', '.join(sorted(available_cuisines)[:10])}...")
        except Exception as e:
            print(f"Impossible de charger la liste des cuisines : {e}")
    
    while True:
        user_input = input("\nType de cuisine (ou appuyez sur entrer pour toutes) : ").strip().lower()
        
        # Cas 1 : L'utilisateur n'a rien saisi 
        if not user_input:
            user_cuisine = ""
            print("Recherche sans filtre de cuisine")
            break
        
        # Cas 2 : En cas de saisie valide, on vérifie si la cuisine existe dans la base de données MongoDB.
        if available_cuisines and user_input in available_cuisines:
            user_cuisine = user_input
            print(f"Recherche des restaurants de type : {user_input.title()}")
            break
        
        # Cas 3 : La cuisine n'existe pas - on propose des suggestions en cas de faute de frappe de l'utilisateur.
        if available_cuisines:
            suggestions = get_cuisine_suggestions(user_input, available_cuisines)
            if suggestions:
                print(f"\nLe type de cuisine '{user_input}' n'existe pas dans notre base.")
                print(f"Suggestions possibles : {', '.join([s.title() for s in suggestions])}")
                print("Voulez-vous utiliser une de ces suggestions ? (o/n)")
                choice = input("Votre choix : ").strip().lower()
                if choice == 'o' and suggestions:
                    user_cuisine = suggestions[0]
                    print(f"Recherche avec : {user_cuisine.title()}")
                    break
        
        # Si pas de suggestions ou refus, on affiche un message et on continue sans filtre.
        print(f"Le type de cuisine '{user_input}' n'existe pas dans notre base.")
        print("Affichage des 3 restaurants les plus proches, toutes categories confondues.")
        user_cuisine = ""
        break
    
    return user_cuisine

user_cuisine = validate_and_get_cuisine()


# --- Fonctions de cache ---

# Fonction pour insérer les résultats dans le cache ou mettre à jour le cache avec les résultats les plus récents.
# Elle nettoie également le cache si nécessaire pour garder un maximum de 20 lignes.
def update_cache(lat, lon, cuisine, results):
    # Si la connexion à PostgreSQL est échue, on ne peut pas mettre à jour le cache.
    if pg_conn is None:
        return
    
    try:
        pg_cursor = pg_conn.cursor()
        
        # On vérifie le nombre de lignes dans le cache pour nettoyer si nécessaire.
        pg_cursor.execute("SELECT COUNT(*) FROM rhistory")
        count = pg_cursor.fetchone()[0]
        
        # Si 20 lignes ou plus, on nettoie le cache
        if count >= 20:
            pg_cursor.execute("DELETE FROM rhistory")
            print("Cache nettoye (20 entrees atteintes)")
        
        # Sinon, on prépare les résultats à stocker (top 3 uniquement)
        results_to_store = results[:3]
        
        # On convertit en JSON pour stockage dans hstore
        results_json = json.dumps(results_to_store, ensure_ascii=False)
        
        # On prépare les données hstore
        # Pour optimiser l'espace de stockage, on stocke les résultats de chaque recherche sous forme de JSON dans une clé unique 'results_json'.
        hstore_dict = {'results_json': results_json}
        
        # On insère les résultats dans le cache
        insert_query = """
            INSERT INTO rhistory (latitude, longitude, cuisine, results)
            VALUES (%s, %s, %s, %s)
        """
        pg_cursor.execute(insert_query, (
            lat, lon, cuisine if cuisine else None, hstore_dict
        ))
        
        pg_cursor.close()
    except Exception as e:
        print(f"Erreur lors de la mise a jour du cache : {e}")


# Fonction pour vérifier si une requête similaire existe dans le cache avec une tolérance de +/-0.001 pour la latitude et la longitude grâce à l'index composite de la table rhistory.
def check_cache(lat, lon, cuisine):
    # Si la connexion à PostgreSQL est échue, on ne peut pas vérifier le cache.
    if pg_conn is None:
        return None
    
    try:
        pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        # RealDictCursor permet de récupérer les résultats sous forme de dictionnaire pour optimiser les performances de la recherche des restaurants
        
        # On applique une tolérance de +/-0.001 pour la latitude et la longitude 
        lat_min = lat - 0.001 
        lat_max = lat + 0.001 
        lon_min = lon - 0.001 
        lon_max = lon + 0.001 
        
        # Construction de la requête selon si cuisine est fournie ou non
        # On utilisera des placeholders %s pour les valeurs des paramètres dans la requête.
        if cuisine:
            # Cas 1 : L'utilisateur a spécifié un type de cuisine
            query = """
                SELECT results
                FROM rhistory 
                WHERE latitude BETWEEN %s AND %s  -- Tolérance de +/-0.001 pour la latitude 
                AND longitude BETWEEN %s AND %s   -- Tolérance de +/-0.001 pour la longitude 
                AND cuisine = %s                   
                ORDER BY created_at DESC          -- On ne récupère que le résultat le plus récent
                LIMIT 1
            """
            pg_cursor.execute(query, (lat_min, lat_max, lon_min, lon_max, cuisine))
        else:
            # Cas 2 : L'utilisateur n'a pas spécifié de cuisine
            query = """
                SELECT results
                FROM rhistory 
                WHERE latitude BETWEEN %s AND %s  
                AND longitude BETWEEN %s AND %s   
                AND cuisine IS NULL                 -- Pas de filtre cuisine (cuisine = NULL dans le cache)
                ORDER BY created_at DESC           
                LIMIT 1
            """
            pg_cursor.execute(query, (lat_min, lat_max, lon_min, lon_max))
        
        result = pg_cursor.fetchone() # Récupération du résultat de la requête.
        pg_cursor.close()
        
        if result:
            hstore_data = result['results'] # Récupération des résultats du cache si trouvés
            results_list = json.loads(hstore_data['results_json'])
            return results_list
        
        return None  # Cache miss : aucun résultat trouvé
    except Exception as e:
        print(f"Erreur lors de la verification du cache : {e}")
        return None



# --- Calcul des distances et récupération des 3 meilleurs restaurants ---

# Fonction pour rechercher les restaurants dans la base de données MongoDB et retourner les 3 meilleurs restaurants en fonction de la distance.
# Elle utilise un heap pour garder seulement les 3 meilleurs résultats au lieu de trier toute la liste à la fin pour optimiser le temps de traitement.
def search_restaurants(user_position, user_cuisine):
    # Si la connexion à MongoDB est échue, on ne peut pas rechercher les restaurants.
    if not mongo_connected:
        return [], 0
    
    # On projette uniquement sur les champs nécessaires pour la recherche des restaurants
    projection = {
        "restaurant_id": 1,
        "name": 1,
        "cuisine": 1,
        "address.coord.coordinates": 1,
        "_id": 0
    }
    
    # On utilisera le module heapq pour garder les 3 restaurants les plus proches
    # Etant donné que heapq suit le classement par ordre croissant, on utilisera la distance négative pour avoir les plus grandes distances en premier

    heap = [] # On initialise une liste vide pour la heap
    total_count = 0 # Compteur pour le nombre total de restaurants correspondant aux critères
    
    try: 
        all_restaurants = collection.find({}, projection) # on lance une requête de sélection de tous les restaurants dans la base de données MongoDB
        
        for resto in all_restaurants:
            # Extraction des coordonnées du restaurant
            try:
                lon_resto = resto['address']['coord']['coordinates'][0]
                lat_resto = resto['address']['coord']['coordinates'][1]
                resto_position = (lat_resto, lon_resto)
            except (KeyError, IndexError, TypeError):
                continue  # On ignore les restaurants sans coordonnées valides
            
            resto_name = resto.get('name', 'Sans nom')
            resto_cuisine = resto.get('cuisine')
            
            # On normalise la cuisine pour la comparaison
            if resto_cuisine is None:
                resto_cuisine_safe = ""
            else:
                resto_cuisine_safe = str(resto_cuisine).strip().lower()
            
            # On vérifie le filtre de cuisine
            condition = (not user_cuisine) or (user_cuisine == resto_cuisine_safe)
            
            if condition:
                # On compte tous les restaurants correspondants (pas seulement les top 3)
                total_count += 1
                
                # On calcule la distance
                distance = geodesic(user_position, resto_position).km
                
                # Principe de notre logique d'optimisation avec la heap :
                # heapq est un "min-heap", c'est-à-dire qu'il classe les éléments par ordre croissant
                # Mais comme on veut garder les 3 PLUS PETITES distances (restaurants les plus proches)
                # et procéder à une comparaison et une mise à jour progressive, on va stocker les distances négatives (-distance) 
                # pour toujours avoir la plus grande distance en premier afin de pouvoir remplacer le restaurant le plus éloigné, si on trouve mieux
                # et d'actualiser le heap avec les 3 meilleurs restaurants.
                
                if len(heap) < 3:
                    # Cas 1 : On a moins de 3 restaurants dans le heap, on ajoute simplement le nouveau restaurant
                    heapq.heappush(heap, (-distance, resto_name, resto_cuisine)) # heapq.heappush ajoute l'élément à la heap et ajuste le classement
                else:
                    # Cas 2 : On a déjà 3 restaurants dans le heap, on compare avec le pire (le plus loin)
                    if distance < -heap[0][0]:  # Comparaison avec la distance du premier tuple (distance, name, cuisine), la plus grande distance
                        heapq.heapreplace(heap, (-distance, resto_name, resto_cuisine)) # heapq.heapreplace remplace l'élément à la heap et ajuste le classement si la nouvelle distance est meilleure (plus petite)
        
        all_restaurants.close()
        
        # Conversion du heap en liste triée :
        # La heap contient des distances négatives triées du plus petit au plus grand négatif
        results = []
        while heap:
            neg_distance, name, cuisine = heapq.heappop(heap)  # Extraction du plus petit au plus grand négatif et suppression de l'élément de la heap
            results.append({
                "name": name,
                "distance_km": -neg_distance,  # On reconvertit en distance positive en appliquant le signe négatif
                "type cuisine": cuisine
            })
        results.reverse() # Inversion pour avoir les plus proches en premier
        
    except Exception as e:
        print(f"Erreur lors de la recherche des restaurants : {e}")
        return [], 0
    
    return results, total_count


# --- Exécution principale ---

# Fonction pour lancer la recherche des restaurants et afficher les résultats.
# Elle vérifie d'abord si une correspondance existe dans le cache avec une tolérance de +/-0.001 pour la latitude et la longitude grâce à l'index composite de la table rhistory.
# Si aucune correspondance n'est trouvée, elle lance la recherche dans la base de données MongoDB et met à jour le cache si nécessaire.
# Elle affiche également les résultats triés par distance croissante.

def main():

    print("\n" + "="*60)
    print("RECHERCHE DE RESTAURANTS")
    print("="*60)

    # On lance le chronomètre
    start_time = time.time()

    # Pour toute recherche, on vérifie d'abord une correspondance dans le cache
    cached_results = check_cache(user_lat, user_lon, user_cuisine)

    if cached_results:
        print("\nResultats trouves dans le cache PostgreSQL")
        results = cached_results
        total_found = len(results)  # Pour le cache, on ne connaît pas le total réel
        cache_hit = True
    else:
        if not mongo_connected:
            print("\nErreur : MongoDB n'est pas accessible")
            print("Impossible de rechercher les restaurants.")
            results = []
            total_found = 0
            cache_hit = False
        else:
            print("\nRecherche dans MongoDB...")
            cache_hit = False
            results, total_found = search_restaurants(user_position, user_cuisine)

    # Affichage des résultats
    if results:
        print("\n" + "="*60)
        print("RÉSULTATS CLASSÉS PAR DISTANCE")
        print("="*60)
        
        # On refait un tri par distance croissante même si on a déjà trié dans la fonction search_restaurants
        if not cache_hit:
            results.sort(key=lambda resto: resto['distance_km'])
        
        # On affiche les 3 restaurants les plus proches
        closest_restaurants = results[:3]
        
        for i, r in enumerate(closest_restaurants, 1):
            distance_str = f"{r['distance_km']:.2f} km"
            cuisine_str = r.get('type cuisine', 'Non specifiee')
            
            print(f"\n{i}. {r['name']}")
            print(f"   Distance : {distance_str}")
            print(f"   Cuisine : {cuisine_str}")
        
        # On met à jour le cache si ce n'était pas un cache hit
        if not cache_hit and results:
            update_cache(user_lat, user_lon, user_cuisine, results)
    else:
        print("\n" + "="*60)
        print("AUCUN RESTAURANT TROUVÉ")
        print("="*60)
        print("Aucun restaurant ne correspond à vos critères de recherche.")

    # Calcul et affichage du temps de traitement
    end_time = time.time()
    execution_time = end_time - start_time

    print("\n" + "="*60)
    print("STATISTIQUES")
    print("="*60)
    print(f"Temps de traitement : {execution_time:.4f} s")
    print(f"Source des donnees : {'Cache PostgreSQL (rhistory)' if cache_hit else 'MongoDB'}")
    if not cache_hit:
        # Pour MongoDB, on affiche le nombre réel de restaurants correspondant aux critères
        print(f"Restaurants trouves : {total_found}")
        # Pour le cache, on ne connaît pas le nombre réel, donc on ne l'affiche pas pour rester fidèle aux résultats réels

    # Fermeture des connexions
    print("\n" + "="*60)
    if pg_conn:
        pg_conn.close()
        print("Connexion PostgreSQL fermee")
    client.close()
    print("Connexion MongoDB fermee")
    print("="*60)

if __name__ == "__main__":
    main()