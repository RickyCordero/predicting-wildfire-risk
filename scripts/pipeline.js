const logger = require('./logger');

const { WILDFIRE_CONFIG, CLIMATE_CONFIG, COMBINE_CONFIG } = require('./config');
const { saveClimateData } = require('./darksky');
const { combineData } = require('./combine');
const { collectWildfireData, createUniqueView, createWildfireView, createStandardizedView } = require('./arcgis');

/**
 * Entry point for the wildfire training data filtering stage.
 * Creates the local arcgis.training collection from the local arcgis.standardized collection.
 */
function createTrainingWildfires() {
    return new Promise((resolve, reject) => {
        const query = WILDFIRE_CONFIG.query;
        const outputDbUrl = WILDFIRE_CONFIG.outputDbUrl;
        const outputDbName = WILDFIRE_CONFIG.outputDbName;
        const outputCollectionName = WILDFIRE_CONFIG.outputCollectionName;
        createStandardizedView(query, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, WILDFIRE_CONFIG.transform);
    });
}

/**
 * Entry point for the climate data collection and saving stage.
 * Creates the local climate.training collection from the local arcgis.training collection.
 */
function saveClimateDataFromTraining() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "arcgis";
        const sourceCollectionName = "training";
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "climate";
        const outputCollectionName = "training";
        saveClimateData(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

module.exports = [];