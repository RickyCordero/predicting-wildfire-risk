const logger = require('./logger');

const { WILDFIRE_CONFIG, CLIMATE_CONFIG, COMBINE_CONFIG } = require('./config');
const { saveClimateData } = require('./darksky');
const { combineData } = require('./combine');
const { loadSave } = require('./db');
const _ = require('lodash');
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

function createTrainingWildfiresMap() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "arcgis";
        const sourceCollectionName = "training";
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "arcgis";
        const outputCollectionName = "map";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            const res = {};
            for (let i = 0; i < docs.length; i++) {
                console.log(i);
                const doc = docs[i];
                const eventId = doc["Event"];
                if (!res[eventId]) {
                    res[eventId] = {};
                }
                res[eventId] = _.omit(doc, "Event");
            }
            cb(null, [res]);
        });
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

function createTrainingClimateMap() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "climate";
        const sourceCollectionName = "training";
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "climate";
        const outputCollectionName = "map";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            const partitions = [];
            const split = 2;
            const p = docs.length / split;
            for (let i = 0; i < split; i++) {
                console.log(i);
                const res = {};
                for (let j = i * p; j < i * p + p; j++) {
                    console.log('-------' + j);
                    const doc = docs[j];
                    const eventId = doc["Event"];
                    if (!res[eventId]) {
                        res[eventId] = {};
                    }
                    res[eventId] = _.omit(doc, "Event");
                }
                partitions.push(res);
            }

            cb(null, partitions);
        });
    });
}

module.exports = [combineData];