import psycopg2
import json
import variables
# =============================================================================
# 1. Configuration des Connexions
# =============================================================================

# --- Configuration PostgreSQL ---
PG_HOST = "localhost"
PG_DATABASE = "sae"  
PG_USER = "postgres"
PG_PASSWORD = variables.mdp 

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "sae_mongo"  # Nom de la base de données MongoDB cible
MONGO_COLLECTION = "restaurants" # Nom de la collection cible

# =============================================================================
# 2. Requête d'Extraction et de Dénormalisation (Extraction)
# =============================================================================

# Cette requête fait une jointure INNER entre vos trois tables sur 'restaurant_id'.
# Elle sélectionne les champs de 'sql_main' et les objets JSON/JSONB de 'sql_geo' et 'sql_feedback'.
PG_QUERY = """
SELECT
    m.restaurant_id,
    m.name,
    m.cuisine,
    m.borough,
    g.address,    -- Données géographiques intégrées
    f.grades      -- Données de feedback intégrées
FROM
    sql_main m
INNER JOIN
    sql_geo g ON m.restaurant_id = g.restaurant_id
INNER JOIN
    sql_feedback f ON m.restaurant_id = f.restaurant_id;
"""

# =============================================================================
# 3. Fonction de Transformation
# =============================================================================

def transform_record_to_document(record):
    """
    Transforme une ligne de résultat PostgreSQL en un document MongoDB.

    Args:
        record (tuple): Une ligne de résultat de la requête PG_QUERY.

    Returns:
        dict: Le document MongoDB dénormalisé.
    """
    (restaurant_id, name, cuisine, borough, address_jsonb, grades_jsonb) = record
    
    # Fonction de conversion des chaînes JSON en objets Python lorsque cela est nécessaire
    def _maybe_load(v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v

    address = _maybe_load(address_jsonb)
    grades = _maybe_load(grades_jsonb)

    # Construction du document MongoDB final
    document = {
        # L'ID interne de MongoDB sera généré automatiquement.
        # Nous conservons l'ID PostgreSQL comme référence.
        "restaurant_id": restaurant_id,
        
        # Champs de la table sql_main
        "name": name,
        "cuisine": cuisine,
        "borough": borough,
        
        # Intégration des données de sql_geo
        "address": address,
        
        # Intégration des données de sql_feedback
        "grades": grades
    }
    
    return document

# =============================================================================
# 4. Fonction Principale de Migration 
# =============================================================================

def migrate_data():
    """
    Orchestre le processus complet d'Extraction, Transformation et Chargement.
    """
    pg_conn = None
    output_fp = None
    
    try:
        # --- Connexion PostgreSQL ---
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        pg_cursor = pg_conn.cursor()
        print(f"Connecté à PostgreSQL BDD: {PG_DATABASE}")

        # --- Préparer le fichier de sortie JSON (one JSON doc per line) ---
        OUTPUT_FILE = "restaurants.json"
        output_fp = open(OUTPUT_FILE, "w", encoding="utf-8")
        print(f"Écriture vers le fichier: {OUTPUT_FILE}")
        
        # --- Exécution de la Requête (Extraction) ---
        pg_cursor.execute(PG_QUERY)
        print("🔍 Requête PostgreSQL exécutée. Début de la transformation... (écriture fichier)")
        
        # --- Transformation et Chargement ---
        BATCH_SIZE = 1000  # Taille du lot pour l'insertion en masse
        documents_to_insert = []
        records_processed = 0
        
        while True:
            # Récupération d'un lot de lignes de PostgreSQL
            records = pg_cursor.fetchmany(BATCH_SIZE)
            if not records:
                break # Sortir si toutes les lignes ont été traitées

            # Transformation des enregistrements en documents MongoDB
            for record in records:
                mongo_document = transform_record_to_document(record)
                documents_to_insert.append(mongo_document)
            
            # Insertion en masse dans MongoDB
            if documents_to_insert:
                # Écriture de chaque document en JSON sur une ligne
                for doc in documents_to_insert:
                    json_line = json.dumps(doc, ensure_ascii=False)
                    output_fp.write(json_line + "\n")
                records_processed += len(documents_to_insert)
                print(f" {records_processed} documents traités et écrits dans le fichier...")
                documents_to_insert = [] # Vider le lot
                
        # --- Fin du Processus ---
        print(f"\nMigration terminée !")
        print(f"Total des documents écrits : {records_processed} dans le fichier '{OUTPUT_FILE}'.")
        
    except psycopg2.Error as e:
        print(f"Erreur PostgreSQL : {e}")
    except Exception as e:
        print(f"Erreur générale (MongoDB ou autre) : {e}")
    finally:
        # Fermeture des ressources
        if pg_conn:
            pg_conn.close()
            print("Connexion PostgreSQL fermée.")
        if output_fp:
            output_fp.close()
            print("Fichier de sortie fermé.")

# =============================================================================
# 5. Exécution et Indexation Post-Migration
# =============================================================================

if __name__ == "__main__":
    migrate_data()
    print("\nFichier NDJSON prêt. Pour importer dans MongoDB utilise :")
    print("mongoimport --db sae_mongo --collection restaurants --file restaurants.ndjson --verbose")
        