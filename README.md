# predicting-wildfire-risk

A machine learning project to predict wildfire size in California using historical meteorological data and historical wildfire event data.

## Getting Started

### Usage

The ```scripts``` folder contains all scripts used to construct the master dataset used for model development.

The ```tf``` folder contains all code related to model development.

## Data Model

The inputs to this model consist of 2 weeks of leading and trailing meteorological factors including temperature, wind, and humidity relative to the ignition date of a wildfire event. The model will seek to approximate a high dimensional mapping from the input space to a real value fire size in acres.

## Data Sources

- Meteorological Data: Dark Sky Time Machine API (https://darksky.net/dev/docs#time-machine-request)
- Wildfire Data: GeoMAC ArcGIS REST API (https://wildfire.cr.usgs.gov/arcgis/rest/services/geomac_dyn/MapServer)