# Projet SpaceX - Pipeline de Données

## Vue d'ensemble
Ce projet implémente un pipeline de données complet pour collecter, transformer et analyser les données des lancements et fusées de SpaceX. Il utilise une architecture moderne ETL (Extract, Transform, Load) avec des outils spécialisés pour chaque étape du processus.

## Architecture
Le pipeline est composé de trois composants principaux :

1. Extraction (E) : Un tap Singer personnalisé ( tap-spacex ) qui extrait les données de l'API SpaceX
2. Chargement (L) : Utilisation de target-snowflake pour charger les données dans une base de données Snowflake
3. Transformation (T) : Modèles dbt pour transformer les données brutes en vues analytiques

## Prérequis
- Python 3.12 ou supérieur
- Compte Snowflake avec les autorisations appropriées
- Environnement virtuel Python

## Installation
# Créer et activer un environnement virtuel
```bash
python -m venv .venv
```
```bash
source .venv\Scripts\activate
```

# Installer les dépendances
```bash
pip install -e .
```

## Configuration
1. Créez un fichier .env dans le dossier tap-spacex avec les variables d'environnement nécessaires
2. Configurez votre fichier config.json avec vos identifiants Snowflake

## Utilisation
### Exécution du pipeline complet
# Exécuter l'extraction et le chargement
```python
python tap-spacex/tap_spacex.py | target-snowflake --config tap-spacex/config.json
```
# Exécuter les transformations dbt
cd dbt/spacex_project
dbt run
dbt test

### Requêtes SQL disponibles
Le dossier sql contient plusieurs requêtes analytiques prêtes à l'emploi pour analyser :

- Les lancements réussis
- Les statistiques par fusée
- Les tendances temporelles des lancements
- Les jointures entre lancements et fusées

## Fonctionnalités
- Extraction automatisée des données de l'API SpaceX (lancements et fusées)
- Chargement des données dans Snowflake
- Transformation des données avec dbt
- Modèles analytiques prêts à l'emploi
- Requêtes SQL d'exemple pour l'analyse
