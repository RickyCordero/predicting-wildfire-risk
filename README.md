# predicting-wildfire-risk

A machine learning project to predict wildfire size in California using historical meteorological data and historical wildfire event data from 2002 - 2019.

## Getting Started

### Usage

This notebook contains all code to analyze and build models from our original dataset.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RickyCordero/predicting-wildfire-risk/blob/master/notebook.ipynb)

The ```climate_scripts``` folder contains all scripts used to construct the dataset used for model development.

## Data Model

The inputs to this model consist of 2 weeks of leading and trailing meteorological factors including temperature, wind, and humidity relative to the ignition date of a wildfire event. The model will seek to approximate a high dimensional mapping from the input space to a real value fire size in acres.

![Alt text](img/relational_training.png?raw=true "Training data table")

![Alt text](img/training_data_geo_distribution.png?raw=true "Training data geographic distribution")

![Alt text](img/fire_sizes.png?raw=true "Fire sizes")

![Alt text](img/log_fire_sizes.png?raw=true "Log normalized fire sizes")

![Alt text](img/3d_correlation.png?raw=true "3d correlation")

## Workflow

![Alt text](img/workflow.png?raw=true "Workflow")

![Alt text](img/loss_curves.png?raw=true "Loss curves")


## Data Sources

- Meteorological Data: Dark Sky Time Machine API (https://darksky.net/dev/docs#time-machine-request)
- Wildfire Data: GeoMAC ArcGIS REST API (https://wildfire.cr.usgs.gov/arcgis/rest/services/geomac_dyn/MapServer)
