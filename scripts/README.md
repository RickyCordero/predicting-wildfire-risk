# scripts

Collects US wildland fire event data spanning 2002-2019. Downloads historical climate data for each event. Combines the datasets into a single training data set to be used for applying machine learning techniques.

## Getting Started

### Prerequisites

- MongoDB 4.0
- NodeJS version >= 8.11.3
- Dark Sky weather API key (https://darksky.net/dev)

### Setup

Install dependencies:

```
npm install
```

Activate an environment with the following variable definitions:
```
MONGODB_URL=YOUR_MONGODB_URL
DARKSKY_API_KEY=YOUR_DARKSKY_API_KEY
```
replacing YOUR_MONGODB_URL with the url of your MongoDB server instance and YOUR_DARKSKY_API_KEY with your Dark Sky API key

Next construct the configuration objects.

### WILDFIRE_CONFIG

The wildfire config object contains the following properties:

* query - The MongoDB query object to specify the subset of unique wildfires to download climate data for

* outputDbUrl - The MongoDB URL
    
* outputDbName - The output database name

* outputCollectionName - The output collection name

* transform - The function to apply to the array of documents in the wildfire collection returned by the above query

### CLIMATE_CONFIG

The climate config object contains the following properties:

* apiKey - The Dark Sky API key string to be used to collect climate data

* interval - Can be ```'daily'``` or ```'hourly'```

* units - The number of units of interval data to collect before each fire's ignition time and after each fire's ignition time, excluding ignition date

* limit - The max number of wildfire events to process in parallel

Examples: To collect ... before and after each event ... 
- 1 day of hourly data, set ```interval``` to ```'hourly'``` and set ```units``` to ```24```
- 1 week of hourly data, set ```interval``` to ```'hourly'``` and set ```units``` to ```168```
- 30 days of daily data, set ```interval``` to ```'daily'``` and set ```units``` to ```30```

### COMBINE_CONFIG

The combine config object contains the following properties:

* units - The number of units used in the climate data collection stage

* props - The climate features to include in the combined training set

Note: when 'daily' data is requested, i.e. ```CLIMATE_CONFIG.interval``` is set to ```'daily'```, 'temperature' (among other properties that are valid for 'hourly' data) is not a valid prop name found in the Dark Sky response object

### Modules

* ./config.js

Specifies variables for each stage in the pipeline

* ./main.js

Represents the point of execution for all processes involved in the data collection and transformation pipeline by executing each stage in the pipeline constructed in ./pipeline.js

* ./pipeline.js

Constructs and exports the sequence of stages to execute sequentially

* ./arcgis.js

Defines the wildfire data collection stage of the pipeline

* ./darksky.js

Defines the climate data collection stage of the pipeline

* ./combine.js

Defines the training data combination stage of the pipeline

* ./queries.js

Contains query objects specifying the filter options used in the wildfire data collection stage of the pipeline

* ./utils.js

Defines helper functions used across each stage of the pipeline

* ./db.js

Defines the database io methods

* ./logger.js

Constructs the logging object

* test/test.js

The set of experiments to be tested

### Running the tests

To run tests, execute the test script
```
npm run test
```

### Pipeline Stages

* collectWildfireData - Entry point for the wildfire data collection stage

1) downloadRaw

Downloads raw wildfire data from the ArcGIS REST API from 2002 to 2018 and saves it to the local arcgis.raw collection

2) createEventsFromRaw

Creates the local arcgis.events collection by extracting all event objects from the raw API response data from the local arcgis.raw collection

3) createUniqueFromEvents

Creates the local arcgis.unique collection by applying the filterMaxInfo function (with an equivalence/hash callback) to the local arcgis.events collection

4) createWildfiresFromUnique

Creates the local arcgis.wildfires collection by filtering out non-wildfire events from the local arcgis.unique collection

5) createStandardizedWildfires

Creates the local arcgis.standardized collection from the local arcgis.wildfires collection. Each document in the resultant collection has uniform key names for a base set of keys defined in each processDoc function.

* createTrainingWildfires - Entry point for the wildfire training data filtering stage

Constructs a standardized view of wildfire events using the configuration properties defined in WILDFIRE_CONFIG by creating the local arcgis.training collection from the local arcgis.standardized collection. The resultant collection represents the set of wildfire events that will be fed into the climate data collection stage of the pipeline.

* saveClimateDataFromTraining - Entry point for the climate data collection and saving stage

Creates the local climate.training collection by downloading climate data for each wildfire event in the local arcgis.training collection

* combineTrainingData - Entry point for the training data combination stage

1) createFilteredPoints

Creates the climate.filteredPoints collection by extracting hourly climate data from the climate.training collection and filtering out all documents that do not have as many hours as requested

2) createWildfirePoints

Creates the training.training collection by finding the corresponding arcgis.training wildfire event for each object from the climate.filteredPoints collection and constructing a new object with properties from both objects

### Usage

In order to build a pipeline, make sure all desired stage functions are in the exports array of ./pipeline.js (in promise form) before executing ./main.js

## Data Sources

- Dark Sky Time Machine API (https://darksky.net/dev/docs#time-machine-request)
- GeoMAC ArcGIS REST API (https://wildfire.cr.usgs.gov/arcgis/rest/services/geomac_dyn/MapServer)