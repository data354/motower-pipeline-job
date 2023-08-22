# OCI GPS ALL JOBS

This project is an ETL (Extract, Transform, Load) pipeline implemented in Python using Airflow. It provides a framework for orchestrating and automating data workflows of GPS project.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Project structures](#project-structure)
- [Dag description](#dag-description)

## Installation

1. clone project

To get started with the project, follow these steps:
```shell
$ git clone https://github.com/data354/OCI-JOBS.git
$ cd OCI-JOBS
```
2. Install required dependencies and activate virtual env (pipenv)
```shell
$ pipenv install
$ pipenv shell
```

## Usage 

Unfortunatly, this project can be run only on OCI env because data sources are only available in this env.

## project-structure

![project structure](/images/structure.png)

config/configs.json contains all project config
dags/common   contains all utils fonctions for our dags
dags/customs_dags  contains all dags declaration


## dags description

This ETL have 2 DAGS 

- DAILY DAG (EXTRACT)
![Daily](/images/daily.png)
- MONTHLY DAG (ENRICH)
![Monthly](/images/monthly_dag.png)


