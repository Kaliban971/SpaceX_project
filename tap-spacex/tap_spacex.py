import singer  # Importation de la bibliothèque Singer pour l'écriture de flux de données
import pandas as pd  # Importation de Pandas pour la manipulation de données
import requests  # Importation de Requests pour effectuer des requêtes HTTP
import io  # Importation de io pour la manipulation de flux de données



LOGGER = singer.get_logger()  # Initialisation du logger pour enregistrer les événements

# Définition du schéma des données
schema = {
    'properties': {
        'id': {'type': 'string'},  # ID du lancement, de type chaîne de caractères
        'name': {'type': 'string'},  # Nom du lancement, de type chaîne de caractères
        'rocket': {'type': 'string'},
        "success": {"type": ["number", "null"]},  # Nom de la fusée, de type chaîne de caractères
        'date_utc': {'type': 'string', 'format': 'date-time'},  # Date du lancement, format date-heure
    },
}

def launches():
    # Récupère les données de l'API SpaceX
    url = requests.get("https://api.spacexdata.com/v4/launches")
    # Lit les données JSON dans un DataFrame Pandas
    df = pd.read_json(io.BytesIO(url.content))
    # Convertit le DataFrame en une liste de dictionnaires
    records = df.to_dict(orient='records')
    # Débogage : Affiche les premiers enregistrements pour vérifier le format
    # print(records[:5])
    # Écrit le schéma et les enregistrements au format Singer
    singer.write_schema('launches', schema, 'id')
    singer.write_records('launches',  records)



def rocket():
    url = requests.get("https://api.spacexdata.com/v4/rockets")
    df = pd.read_json(io.BytesIO(url.content))

    schema = {
        'properties': {
            'id': {'type':'string'},
            'name': {'type':'string'},
            'active': {'type':'boolean'},
        },
    }


    records = df.to_dict(orient='records')
    singer.write_schema('rockets', schema, 'id')
    singer.write_records('rockets',  records)

def main():
    launches()  # Appelle la fonction pour récupérer et écrire les données de lancement
    rocket()  # Appelle la fonction pour récupérer et écrire les données de fusée

if __name__ == "__main__":
    main()  # Appelle la fonction principale pour exécuter le script