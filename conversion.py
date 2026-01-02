"""
Script de conversion des données de la base de données PostgreSQL vers la base de données MongoDB.

Nous avons créé la base de données postgres sae dans un premier temps pour faciliter la migration 
vers mongoDB. Pour ça, nous avons utilisé le script insert_queries.sql pour insérer 
les données dans la base de données postgres. Voici les requêtes utilisées :
CREATE DATABASE sae;
\c sae;
\i insert_queries.sql;
\q
"""

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
MONGO_DATABASE = "sae_mongo"  # Nom de notre base de données MongoDB pour la migration
MONGO_COLLECTION = "restaurants" # Nom de la collection des documents comme demandé dans l'énoncé

# =============================================================================
# 2. Requêtes d'Extraction
# =============================================================================

# Cette requête fait une jointure INNER entre les trois tables SQL sql_main, sql_geo et sql_feedback 
# sur la clé restaurant_id et permet de récupérer les données de chaque restaurant.

PG_QUERY = """
SELECT
    m.restaurant_id,
    m.name,
    m.cuisine,
    m.borough,
    g.address,    -- Les données géographiques contenues dans la table sql_geo
    f.grades      -- Les données de feedback contenues dans la table sql_feedback
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
        dict: Le document MongoDB contenant les données de chaque restaurant sous forme de dictionnaire.
    """
    (restaurant_id, name, cuisine, borough, address_jsonb, grades_jsonb) = record
    
    # Fonction de conversion des chaînes JSON en objets Python pour les données de type JSONB.

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
        # Vu que l'ID interne de MongoDB est généré automatiquement,
        # nous conservons l'ID PostgreSQL comme référence.
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

    Args:
        None : Aucun argument

    Returns:
        None : Aucun retour

    Raises:
        psycopg2.Error: Si une erreur survient lors de la connexion à PostgreSQL
        Exception: Si une erreur survient lors de la migration des données
    """

    # Initialisation des connexions
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

        # --- Préparation du fichier de sortie JSON (un document JSON par ligne) ---
        OUTPUT_FILE = "restaurants.json"
        output_fp = open(OUTPUT_FILE, "w", encoding="utf-8")
        print(f"Écriture vers le fichier: {OUTPUT_FILE}")
        
        # --- Exécution de la requête SQL pour récupérer les données de chaque restaurant ---
        pg_cursor.execute(PG_QUERY)
        print("Requête PostgreSQL exécutée. Début de la transformation... (écriture dans le fichier)")
        
        # --- Transformation et Chargement ---
        # Au lieu d'insérer chaque document dans MongoDB une par une, on va insérer les documents par lots de 1000.
        # De quoi optimiser le temps d'exécution de la migration.
        BATCH_SIZE = 1000  
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
    print("\nFichier JSON prêt. Prochaine étape : importation dans MongoDB via la commande suivante :")
    print("mongoimport --db sae_mongo --collection restaurants --file restaurants.json --verbose")
        