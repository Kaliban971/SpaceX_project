# Importe la bibliothèque 'polars' et lui donne l'alias 'pl' (plus commun que 'pd' pour polars)
import polars as pl
# Importe la bibliothèque 'requests' pour faire des requêtes HTTP (aller chercher des données sur le web)
import requests
# Importe le module 'io' pour travailler avec des flux de données en mémoire (comme des fichiers virtuels)
import io
# Importe le module 'sys' et 'codecs' pour gérer l'encodage de la sortie console sur Windows
import sys
import codecs

# --- Configuration de l'encodage pour la console Windows ---
# S'assure que la console peut afficher correctement les caractères spéciaux (comme les accents ou symboles)
# en forçant l'utilisation de l'encodage UTF-8 si ce n'est pas déjà le cas.
if sys.stdout.encoding != 'utf-8':
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
# --- Fin de la configuration de l'encodage ---

def fetch_and_display_spacex_data():
  """
  Fonction principale pour récupérer les données des lancements SpaceX via l'API,
  les charger dans un DataFrame Polars et afficher des informations de base.
  """
  # Définit l'adresse (URL) de l'API SpaceX pour obtenir la liste de tous les lancements.
  url = "https://api.spacexdata.com/v4/launches"
  print(f"Tentative de récupération des données depuis : {url}")

  try:
    # --- Étape 1: Récupérer les données avec 'requests' ---
    # Envoie une requête GET à l'URL pour demander les données.
    response = requests.get(url)
    # Vérifie si la requête a réussi. Si l'API renvoie une erreur (ex: page non trouvée 404, erreur serveur 500),
    # cela lèvera une exception et arrêtera le script ici.
    response.raise_for_status()
    print("Données récupérées avec succès depuis l'API.")

    # --- Étape 2: Lire les données JSON en mémoire avec Polars ---
    # 'response.content' contient les données brutes (en bytes) reçues.
    # 'io.BytesIO' transforme ces bytes en un objet que Polars peut lire comme un fichier.
    # 'pl.read_json' lit ces données et les transforme en DataFrame.
    # 'infer_schema_length=1000' aide Polars à mieux deviner le type des colonnes,
    # surtout si certaines commencent par des valeurs manquantes (null).
    df = pl.read_json(io.BytesIO(response.content), infer_schema_length=1000)
    print("Données chargées avec succès dans un DataFrame Polars.")

    # --- Étape 3: Afficher les informations du DataFrame ---
    # Affiche les 5 premières lignes du tableau pour avoir un aperçu.
    print("\n--- Aperçu des 5 premières lignes (head) ---")
    print(df.head())

    # Affiche la liste des noms de toutes les colonnes du tableau.
    print("\n--- Noms des colonnes (columns) ---")
    print(df.columns)

    # Affiche le type de données détecté par Polars pour chaque colonne (ex: texte, nombre entier, date...).
    print("\n--- Types de données des colonnes (dtypes) ---")
    print(df.dtypes)

  except requests.exceptions.RequestException as e:
    # Gère les erreurs spécifiques qui peuvent survenir lors de la requête web (problème de connexion, URL invalide...).
    print(f"\nErreur lors de la requête vers l'API : {e}")
  except Exception as e:
    # Gère toutes les autres erreurs imprévues qui pourraient survenir (ex: format JSON incorrect, problème Polars...).
    print(f"\nUne erreur inattendue est survenue : {e}")

# Ce bloc vérifie si le script est exécuté directement (pas importé comme module).
# Si oui, il appelle la fonction principale pour lancer le processus.
if __name__ == "__main__":
  fetch_and_display_spacex_data()