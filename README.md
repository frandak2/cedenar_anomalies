# cedenar_anomalies

ste proyecto se enfoca en el análisis y procesamiento de datos relacionados con Zentry y CEDENAR, con el objetivo de identificar anomalías y realizar una clasificación mediante técnicas de clustering difuso. El proyecto integra diferentes fuentes de datos para crear un modelo predictivo y generar informes descriptivos.
  
## Installation guide

Please read [install.md](install.md) for details on how to set up this project.

## Project Organization

    ├── alembic.ini
    ├── .env          
    ├── README.md          <- The top-level README for Data Scientist using this project.
    ├── poetry.lock
    ├── pyproject.toml
    ├── venv
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── migrations         <- Folder create by ini alembic
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-fmh-initial-data-exploration`.
    │
    ├── postgresql         <- folder of DB.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures         <- Generated graphics and figures to be used in reporting.
    │
    ├── environment.yml    <- The requirements file for reproducing the analysis environment.
    │
    ├── .here              <- File that will stop the search if none of the other criteria
    │                         apply when searching head of project.
    │
    ├── setup.py           <- Makes project pip installable (pip install -e .)
    │                         so cedenar_anomalies can be imported.
    │
    └── cedenar_anomalies               <- Source code for use in this project.
        ├── __init__.py
        ├── main.py
        ├── domain/
        │   ├── __init__.py
        │   ├── models
        │   │   ├── __init__.py
        │   │   └── models.py
        │   ├── ports
        │   │   ├── __init__.py
        │   │   └── repositories.py
        │   └── services
        │       ├── __init__.py
        │       ├── data_processing_service.py
        │       ├── anomalia_service.py
        │       └── data_cleaning_service.py
        ├── application
        │   ├── __init__.py
        │   ├── load_to_db.py
        │   ├──load_from_api.py
        │   └── make_dataset.py
        ├── infrastructure
        │    ├── __init__.py
        │    ├── database
        │    │   ├── __init__.py
        │    │   ├── session.py
        │    │   ├── init_db.py
        │    │   └── models.py
        │    └── adapters
        │        ├── __init__.py
        │        ├── api
        │        │   ├── __init__.py
        │        │   └── anomalia_api_client.py
        │        └── repositories
        │            ├── __init__.py
        │            ├── sql_anomalia_repository.py
        │            └── async_sql_anomalia_repository.py
        └── utils
           ├── __init__.py
           ├──async_alembic.py
           ├──config.py
           └──logging_config.py


---
Project based on the [cookiecutter conda data science project template](https://github.com/frandak2/cookiecutter-personal).