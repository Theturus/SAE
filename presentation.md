# Présentation du Projet : Application de Recherche de Restaurants

## 1. Introduction

Ce projet consiste en le développement d'une application de recherche de restaurants géolocalisés utilisant une architecture hybride combinant PostgreSQL (base relationnelle) et MongoDB (base NoSQL). L'objectif principal est de permettre aux utilisateurs de trouver les 3 restaurants les plus proches d'une position géographique donnée, avec possibilité de filtrage par type de cuisine.

L'application met en œuvre plusieurs concepts clés :
- **Migration de données** : Conversion d'une base relationnelle PostgreSQL vers MongoDB
- **Dénormalisation** : Transformation de données normalisées en documents MongoDB
- **Mise en cache** : Utilisation de PostgreSQL avec l'extension `hstore` pour optimiser les performances
- **Optimisations** : Utilisation de structures de données efficaces (heap) et d'indexation pour améliorer les temps de réponse

## 2. La Base Relationnelle et les Données Initiales

### 2.1 Structure de la Base PostgreSQL

La base de données relationnelle PostgreSQL (`sae`) est organisée en trois tables principales normalisées :

#### Table `sql_main`
Contient les informations principales des restaurants :
- `restaurant_id` : Identifiant unique du restaurant (clé primaire)
- `name` : Nom du restaurant
- `cuisine` : Type de cuisine
- `borough` : Quartier où se trouve le restaurant

#### Table `sql_geo`
Contient les données géographiques :
- `restaurant_id` : Clé étrangère vers `sql_main`
- `address` : Données d'adresse au format JSONB contenant :
  - `street` : Nom de la rue
  - `zipcode` : Code postal
  - `building` : Numéro du bâtiment
  - `coord` : Coordonnées géographiques (latitude, longitude)

#### Table `sql_feedback`
Contient les évaluations et notes des restaurants :
- `restaurant_id` : Clé étrangère vers `sql_main`
- `grades` : Liste des évaluations au format JSONB, chaque évaluation contenant :
  - `date` : Date de l'évaluation
  - `grade` : Note attribuée (A, B, C, D, F)
  - `score` : Score numérique associé

### 2.2 Relations entre les Tables

Les trois tables sont liées par des **jointures INNER** sur la clé `restaurant_id`, garantissant que seuls les restaurants ayant des données complètes (informations principales, géolocalisation et évaluations) sont pris en compte.

### 2.3 Chargement Initial des Données

Les données initiales sont chargées dans PostgreSQL via le script `insert_queries.sql` qui contient les instructions INSERT pour peupler les trois tables.

## 3. Les Deux Bases NoSQL et le Formatage des Données

### 3.1 Base MongoDB : Collection `restaurants`

#### 3.1.1 Dénormalisation des Données

La migration vers MongoDB implique une **dénormalisation** des données : les informations dispersées dans trois tables PostgreSQL sont regroupées en un seul document MongoDB.

#### 3.1.2 Structure des Documents MongoDB

Chaque document de la collection `restaurants` a la structure suivante :

```json
{
  "_id": ObjectId("..."),
  "restaurant_id": 30112340,
  "name": "Wendy S",
  "cuisine": "Hamburgers",
  "borough": "Brooklyn",
  "address": {
    "coord": {
      "type": "Point",
      "coordinates": [-73.961704, 40.662942]
    },
    "street": "Flatbush Avenue",
    "zipcode": "11225",
    "building": "469"
  },
  "grades": [
    {
      "date": ISODate("2014-12-30T00:00:00.000Z"),
      "grade": "A",
      "score": 8
    },
    ...
  ]
}
```

#### 3.1.3 Avantages de la Dénormalisation

- **Performance** : Une seule requête suffit pour récupérer toutes les informations d'un restaurant
- **Simplicité** : Structure de données plus intuitive et alignée avec les besoins applicatifs
- **Scalabilité** : MongoDB est optimisé pour ce type de structure documentaire

### 3.2 Base PostgreSQL : Table `rhistory` (Cache)

#### 3.2.1 Structure de la Table de Cache

La table `rhistory` est utilisée pour mettre en cache les résultats de recherche :

```sql
CREATE TABLE rhistory (
    id SERIAL PRIMARY KEY,
    latitude NUMERIC(10, 6) NOT NULL,
    longitude NUMERIC(10, 6) NOT NULL,
    cuisine VARCHAR(255),
    results HSTORE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 3.2.2 Formatage des Données dans HSTORE

Les résultats sont stockés dans la colonne `results` de type `HSTORE` au format JSON :

```python
hstore_dict = {
    'results_json': '[{"name":"Restaurant A","distance_km":1.5,...}, {...}, {...}]'
}
```

**Avantages de ce format** :
- **Compact** : Tous les résultats dans une seule clé
- **Facile à parser** : Utilisation directe de `json.loads()`
- **Extensible** : Possibilité d'ajouter des champs sans modifier la structure

#### 3.2.3 Indexation pour Optimisation

Deux index sont créés pour optimiser les recherches dans le cache :

1. **Index composite** `idx_rhistory_coords_cuisine` sur `(latitude, longitude, cuisine)`
   - Permet des recherches rapides par coordonnées et type de cuisine

2. **Index** `idx_rhistory_created` sur `created_at DESC`
   - Accélère le tri par date de création pour récupérer les résultats les plus récents

#### 3.2.4 Politique de Gestion du Cache

- **Taille maximale** : 20 entrées maximum
- **Éviction** : Lorsque 20 entrées sont atteintes, le cache est entièrement vidé (`DELETE FROM rhistory`)
- **Tolérance géographique** : Les recherches sont considérées comme similaires si les coordonnées diffèrent de moins de ±0.001 degré (environ 111 mètres)

## 4. L'Application `conversion.py`

### 4.1 Objectif

Le script `conversion.py` réalise la migration des données de PostgreSQL vers MongoDB en suivant un processus ETL (Extract, Transform, Load).

### 4.2 Architecture ETL

#### 4.2.1 Extraction (Extract)

Une requête SQL avec jointures INNER récupère toutes les données nécessaires :

```sql
SELECT
    m.restaurant_id, m.name, m.cuisine, m.borough,
    g.address,    -- Données géographiques
    f.grades      -- Données de feedback
FROM sql_main m
INNER JOIN sql_geo g ON m.restaurant_id = g.restaurant_id
INNER JOIN sql_feedback f ON m.restaurant_id = f.restaurant_id;
```

Cette requête garantit que seuls les restaurants avec données complètes sont migrés.

#### 4.2.2 Transformation (Transform)

La fonction `transform_record_to_document()` convertit chaque ligne PostgreSQL en document MongoDB :

1. **Parsing des données JSONB** : Les champs `address` et `grades` sont convertis de JSONB vers des dictionnaires Python
2. **Construction du document** : Création d'un document MongoDB dénormalisé contenant toutes les informations
3. **Conservation de l'ID** : Le `restaurant_id` PostgreSQL est conservé comme référence

#### 4.2.3 Chargement (Load)

Les documents sont écrits dans un fichier JSON au format "one JSON document per line" :

- **Traitement par lots** : Les données sont traitées par lots de 1000 enregistrements pour optimiser la mémoire
- **Format de sortie** : Chaque ligne du fichier `restaurants.json` contient un document JSON complet
- **Import MongoDB** : Le fichier est ensuite importé dans MongoDB via `mongoimport`

### 4.3 Avantages de cette Approche

- **Séparation des préoccupations** : Migration isolée de l'application principale
- **Réutilisabilité** : Le fichier JSON peut être réimporté si nécessaire
- **Traçabilité** : Possibilité de vérifier les données avant import
- **Performance** : Traitement par lots optimise l'utilisation mémoire

## 5. L'Application Finale `sae.py`

### 5.1 Architecture Générale

L'application `sae.py` est structurée en plusieurs sections :

1. **Connexions aux bases de données** (PostgreSQL et MongoDB)
2. **Saisie et validation des coordonnées utilisateur**
3. **Gestion du filtre de cuisine avec suggestions**
4. **Système de cache PostgreSQL**
5. **Recherche optimisée dans MongoDB**
6. **Affichage des résultats et statistiques**

### 5.2 Fonctionnalités Principales

#### 5.2.1 Validation des Coordonnées

- **Plages de validation** :
  - Latitude : [40.50, 41.20]
  - Longitude : [-74.26, -73.20]
- **Gestion d'erreurs** : Validation du format et de la plage de valeurs
- **Messages d'erreur clairs** : Distinction entre erreurs de format et de valeur

#### 5.2.2 Gestion Intelligente du Filtre de Cuisine

- **Vérification d'existence** : Vérification que le type de cuisine existe dans MongoDB
- **Suggestions automatiques** : Utilisation de `difflib.get_close_matches()` pour proposer des corrections en cas de faute de frappe
- **Recherche sans filtre** : Si la cuisine n'existe pas, recherche de tous les restaurants sans filtre

#### 5.2.3 Système de Cache PostgreSQL

**Fonction `check_cache(lat, lon, cuisine)`** :
- Recherche dans `rhistory` avec tolérance de ±0.001 sur les coordonnées
- Utilisation de `BETWEEN` pour permettre l'utilisation de l'index composite
- Retourne les résultats en cache si trouvés, `None` sinon

**Fonction `update_cache(lat, lon, cuisine, results)`** :
- Stocke les top 3 résultats au format JSON dans `hstore`
- Gère l'éviction automatique (nettoyage à 20 entrées)
- Utilise `autocommit` pour validation automatique des transactions

#### 5.2.4 Recherche Optimisée dans MongoDB

**Fonction `search_restaurants(user_position, user_cuisine)`** :

**Optimisations implémentées** :

1. **Projection MongoDB** : Seuls les champs nécessaires sont récupérés
   ```python
   projection = {
       "restaurant_id": 1, "name": 1, "cuisine": 1,
       "address.coord.coordinates": 1, "_id": 0
   }
   ```

2. **Utilisation d'un heap (min-heap)** :
   - **Principe** : Au lieu de trier tous les restaurants à la fin, on maintient seulement les 3 meilleurs en temps réel
   - **Astuce** : Stockage des distances négatives pour que `heap[0]` contienne toujours le restaurant le plus éloigné (le pire)
   - **Avantage** : Coût computationnel réduit

3. **Comptage des résultats totaux** :
   - Compte tous les restaurants correspondant aux critères
   - Permet d'afficher une statistique pertinente : "X restaurants trouvés, top 3 affichés"

**Algorithme du heap** :
```python
if len(heap) < 3:
    heapq.heappush(heap, (-distance, resto_name, resto_cuisine))
else:
    if distance < -heap[0][0]:  # Si meilleur que le pire
        heapq.heapreplace(heap, (-distance, resto_name, resto_cuisine))
```

### 5.3 Flux d'Exécution

1. **Initialisation** : Connexions aux bases de données et création des structures nécessaires
2. **Saisie utilisateur** : Validation des coordonnées et du filtre de cuisine
3. **Vérification du cache** : Recherche dans `rhistory` avec tolérance géographique
4. **Si cache hit** : Retour immédiat des résultats
5. **Si cache miss** : 
   - Recherche dans MongoDB avec heap
   - Calcul des distances pour tous les restaurants correspondants
   - Sélection des 3 meilleurs
   - Mise à jour du cache
6. **Affichage** : Présentation des résultats avec statistiques

### 5.4 Gestion des Erreurs

- **Connexion MongoDB** : Vérification avec `ping()` avant utilisation
- **Connexion PostgreSQL** : Gestion si la connexion échoue (cache désactivé)
- **Coordonnées manquantes** : Ignore les restaurants sans coordonnées valides
- **Erreurs de cache** : Retourne `None` en cas d'erreur, permettant un fallback vers MongoDB

## 6. Optimisations Implémentées

### 6.1 Optimisations MongoDB

- **Projection** : Réduction de la bande passante en ne récupérant que les champs nécessaires
- **Heap** : Maintien de seulement 3 résultats au lieu de trier toute la collection
- **Fermeture explicite** : `all_restaurants.close()` pour libérer les ressources

### 6.2 Optimisations PostgreSQL (Cache)

- **Index composite** : Recherche rapide par coordonnées et cuisine
- **Index sur created_at** : Tri rapide pour récupérer le résultat le plus récent
- **Requête BETWEEN** : Utilisation de `BETWEEN` pour permettre l'utilisation de l'index
- **ANALYZE** : Mise à jour des statistiques pour optimiser les plans d'exécution
- **Format JSON** : Stockage compact et facile à parser

### 6.3 Optimisations Algorithmiques

- **Heap vs Tri complet** : Complexité réduite
- **Comptage incrémental** : Comptage des résultats totaux sans impact significatif sur les performances
- **Normalisation des cuisines** : Conversion en minuscules une seule fois pour comparaisons efficaces

## 7. Résultats et Performances

### 7.1 Temps d'Exécution

- **Cache hit** : Temps de réponse très rapide (< 0.01s) grâce à la lecture directe depuis PostgreSQL
- **Cache miss** : Temps dépendant de la taille de la collection MongoDB, optimisé par le heap

### 7.2 Statistiques Affichées

- **Temps de traitement** : Mesure précise du temps d'exécution
- **Source des données** : Indication claire si les résultats viennent du cache ou de MongoDB
- **Restaurants trouvés** : Nombre réel de restaurants correspondant aux critères (uniquement pour MongoDB)

## 8. Conclusion

Ce projet démontre l'intérêt d'une architecture hybride combinant les forces de PostgreSQL et MongoDB :

### 8.1 Points Forts de l'Architecture

1. **Flexibilité** : MongoDB permet une structure de données adaptée aux besoins applicatifs
2. **Performance** : Le cache PostgreSQL réduit significativement les temps de réponse
3. **Scalabilité** : L'architecture peut gérer de grandes collections de restaurants
4. **Maintenabilité** : Code structuré et bien documenté

### 8.2 Apports Techniques

- **Migration ETL** : Démonstration d'un processus de migration structuré
- **Dénormalisation** : Compréhension des avantages et inconvénients
- **Optimisations** : Mise en pratique de techniques d'optimisation (heap, indexation)
- **Gestion de cache** : Implémentation d'un système de cache efficace avec éviction

Ce projet illustre efficacement les concepts de bases de données NoSQL et démontre comment combiner différents systèmes de gestion de bases de données pour créer une application performante et évolutive.

