# Warframe Data Extract

An apache airflow DAG that utilizes the following:

- Connections to an https endpoint serving a monolithic, rendered HTML document containing the target data
- Transformation of said data into standard database tables, rows and columns
- Extraction to a local database to power various Apache Superset dashboards

## Endpoints Used
Warframe Drop Tables are obtained from the official Digital Extremes (DE) drop tables, hosted here: https://www.warframe.com/droptables

The data therein is sorted into HTML tables, in a singular, monolithic format.

## Target Data
The goal is to sort the data into an easily understandable database format.
Ideally, the structure will be as follows:

- Planet Objects
- Mission Objects
- Reward Objects

Missions can have multiple rewards; planets can have multiple missions.

## End Goal
End goal is to have a dashboard, primarily for Relics, then later for mods, items and cache rewards; that can be
easily searched through or sorted, enabling players to find the highest drop-chance mission for their desired reward.