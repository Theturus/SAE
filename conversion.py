"""
Notre script de conversion des données de la base de données PostgreSQL vers la base de données MongoDB

Nous avons créé la base de données postgresql sae dans un premier temps pour faciliter la migration 
vers mongoDB. Pour ça, nous avons utilisé le script insert_queries.sql pour insérer 
les données une fois les bases et les tables créées. Voici les requêtes utilisées :

CREATE DATABASE sae;
\c sae;
\i insert_queries.sql;
\q

"""

import psycopg2
import json
import variables


# --- Configuration des Connexions ---

# Configuration PostgreSQL
PG_HOST = "localhost"
PG_DATABASE = "sae"  
PG_USER = "postgres"
PG_PASSWORD = variables.mdp 

# Configuration MongoDB
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DATABASE = "sae_mongo"  # Nom de notre base de données MongoDB pour la migration
MONGO_COLLECTION = "restaurants" # Nom de la collection des documents comme demandé dans le cahier des charges


# --- Requêtes d'Extraction ---

# Ici, on utilise une requête SQL qui fait une jointure INNER entre les trois tables SQL sql_main, sql_geo et sql_feedback 
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

# --- Fonction de Transformation ---

# Pour simplifier la structure de nos données dans MongoDB, on va transformer chaque ligne de résultat PostgreSQL en un document MongoDB.
# De sorte, on aura un document MongoDB par restaurant. 

def transform_record_to_document(record):
    (restaurant_id, name, cuisine, borough, address_jsonb, grades_jsonb) = record
    
    # Ici, on utilise une fonction de conversion des chaînes JSON en objets Python pour les données de type JSONB pour les champs address et grades.
    def to_dict(v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v

    address = to_dict(address_jsonb)
    grades = to_dict(grades_jsonb)

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


# --- Fonction Principale de Migration ---

# Ici, on définit la fonction principale de migration qui va récupérer les données de la base de données PostgreSQL et les transformer en documents MongoDB.
# Puis, on va écrire ces documents dans un fichier JSON.

def migrate_data():
    try:
        # Connexion à la base de données PostgreSQL
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        pg_cursor = pg_conn.cursor()
        print(f"Connecté à la base de données PostgreSQL: {PG_DATABASE}")

        # Préparation du fichier de sortie JSON (un document JSON par ligne)
        OUTPUT_FILE = "restaurants.json"
        output_fp = open(OUTPUT_FILE, "w", encoding="utf-8")
        print(f"Écriture vers le fichier JSON: {OUTPUT_FILE}")
        
        # Exécution de la requête SQL pour récupérer les données de chaque restaurant
        pg_cursor.execute(PG_QUERY)
        print("Requête SQL exécutée. Début de la transformation... (écriture dans le fichier)")
        
        # Transformation et Chargement
        # Au lieu d'insérer chaque document dans MongoDB une par une, on va insérer les documents par lots de 1000 pour optimiser le temps d'exécution de la migration.
        BATCH_SIZE = 1000  
        documents_to_insert = []
        records_processed = 0
        
        while True:
            # Récupération d'un lot de lignes de PostgreSQL
            records = pg_cursor.fetchmany(BATCH_SIZE)
            if not records:
                break # On sort de la boucle while si toutes les lignes ont été traitées

            # Transformation des enregistrements en documents MongoDB
            for record in records:
                mongo_document = transform_record_to_document(record)
                documents_to_insert.append(mongo_document)
            
            # Insertion en masse dans MongoDB
            if documents_to_insert:
                # Écriture de chaque document en JSON sur une ligne dans le fichier JSON
                for doc in documents_to_insert:
                    json_line = json.dumps(doc, ensure_ascii=False)
                    output_fp.write(json_line + "\n")
                records_processed += len(documents_to_insert)
                print(f" {records_processed} documents traités et écrits dans le fichier...")
                documents_to_insert = [] # On vide le lot
                
        # Fin du Processus
        print(f"\nMigration terminée !")
        print(f"Total des documents écrits : {records_processed} dans le fichier '{OUTPUT_FILE}'.")
        
    # Gestion des erreurs lors de la connexion à la base de données PostgreSQL ou MongoDB ou lors de la migration des données.
    # Si une erreur survient, on affiche un message d'erreur et on ferme les connexions.
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


# --- Exécution et Indexation Post-Migration ---

if __name__ == "__main__":
    migrate_data()
    print("\nFichier JSON prêt. Prochaine étape : importation dans MongoDB via la commande suivante :")
    print("mongoimport --db sae_mongo --collection restaurants --file restaurants.json --verbose")
        