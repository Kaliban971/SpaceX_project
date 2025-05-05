# Importe la bibliothèque singer, un outil pour formater la sortie des programmes d'extraction de données (ETL).
import singer
# Importe la bibliothèque polars (avec le petit nom 'pl'), super efficace pour manipuler des tableaux de données.
import polars as pl
# Importe la bibliothèque requests, notre "coursier" pour aller chercher des informations sur internet.
import requests
# Importe le module io, utile pour traiter des données en mémoire comme si c'étaient des fichiers.
import io
# Importe les modules sys et codecs, nos outils pour que la console Windows affiche correctement les caractères spéciaux (accents, etc.).
import sys
import codecs

# --- Configuration de l'encodage pour la console Windows ---
# On vérifie si la console utilise déjà le bon "alphabet" (UTF-8).
if sys.stdout.encoding != 'utf-8':
  # Si non, on lui dit d'utiliser UTF-8 pour être sûr de bien tout afficher.
  # On prend la sortie brute (buffer) et on l'enveloppe dans un "traducteur" UTF-8.
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
# --- Fin de la configuration de l'encodage ---

# Définit le "plan" ou la structure des données que l'on souhaite extraire.
# C'est comme dire : "Je veux seulement ces informations-là : id, nom, et date".
schema = {
    # 'properties' décrit les colonnes que l'on veut.
    'properties': {
        # 'id' : une colonne pour l'identifiant unique (du texte).
        'id': {'type': 'string'},
        # 'name' : une colonne pour le nom de la mission (du texte).
        'name': {'type': 'string'},
        # 'date_utc' : une colonne pour la date et l'heure (du texte, mais formaté comme une date).
        'date_utc': {'type':'string', 'format': 'date-time'},
    }
}

# Définit la fonction principale qui fait tout le travail.
def get_spacex_data():
    """
    Fonction qui récupère les données des lancements SpaceX depuis l'API v4,
    les charge dans un tableau Polars, puis les affiche au format Singer.
    ATTENTION : Sans gestion d'erreurs (try...except), le script plantera si un problème survient.
    """
    # L'adresse internet (URL) où se trouvent les informations sur tous les lancements SpaceX (version 4 de l'API).
    url = "https://api.spacexdata.com/v4/launches"
    # Affiche l'URL qu'on va contacter (utile pour savoir ce qui se passe).
    print(f"Récupération des données depuis : {url}", file=sys.stderr) # Écrit sur la sortie d'erreur pour ne pas interférer avec la sortie Singer

    # --- Étape 1: Récupérer les données avec 'requests' ---
    # On envoie notre "coursier" (requests) chercher les données à l'adresse 'url'.
    response = requests.get(url)
    # On demande au coursier de vérifier si tout s'est bien passé (pas d'erreur 404 ou autre).
    # S'il y a eu un problème, le script s'arrêtera ici.
    response.raise_for_status()

    # --- Étape 2: Lire les données JSON en mémoire avec Polars ---
    # On prend le contenu brut ramené par le coursier ('response.content').
    # On utilise 'io.BytesIO' pour que Polars puisse le lire comme un fichier en mémoire.
    # On demande à Polars ('pl') de lire ces données JSON et de les mettre dans un tableau ('df').
    # 'infer_schema_length=1000' : On dit à Polars de regarder les 1000 premières lignes pour bien deviner le type de chaque colonne.
    df = pl.read_json(io.BytesIO(response.content), infer_schema_length=1000)

    # --- Étape 3: Préparer et écrire les données au format Singer ---
    # On transforme notre tableau Polars ('df') en une liste de dictionnaires Python.
    # Chaque dictionnaire représente une ligne (un lancement). 'to_dicts()' est la méthode moderne.
    try:
        # Essaye d'utiliser la méthode la plus récente et efficace.
        records = df.to_dicts()
    except AttributeError:
        # Si 'to_dicts()' n'existe pas (ancienne version de Polars), utilise une méthode de repli.
        # 'as_series=False' est nécessaire pour certaines versions intermédiaires.
        records = df.to_dict(as_series=False)

    # On utilise 'singer' pour écrire d'abord le "plan" (schema) sur la sortie standard.
    # 'launches' est le nom de notre flux de données, 'id' est la clé unique.
    singer.write_schema('launches', schema, 'id')

    # Ensuite, on utilise 'singer' pour écrire toutes les données (les 'records') sur la sortie standard.
    singer.write_records('launches', records)

    # Affiche un message final sur la sortie d'erreur pour indiquer que le travail est fait.
    print(f"Traitement terminé. {len(records)} enregistrements écrits pour le flux 'launches'.", file=sys.stderr)


# Cette partie spéciale vérifie si on exécute ce fichier directement.
if __name__ == '__main__':
    # Si oui, on appelle notre fonction principale pour lancer le processus.
    get_spacex_data()

# Le résultat principal (schéma et enregistrements au format Singer) sera affiché sur la sortie standard (stdout).
# Les messages de statut (comme l'URL et le message final) sont affichés sur la sortie d'erreur (stderr).