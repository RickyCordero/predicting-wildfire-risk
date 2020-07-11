const logger = require('./logger');

const _ = require('lodash');
const { COMBINE_CONFIG } = require('./config');
const { loadTransform, streamSave } = require('./db');

/**
 * Creates the training.training collection by expanding climate attributes
 * into columns with time indexed labels using the climate.training2 collection
 *  and arcgis.map collection.
 */
function createTrainingTraining() { // uses streams
    return new Promise((resolve, reject) => {
        const climateQuery = {};
        const climateProjection = {};

        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "training2";

        const wildfireQuery = {};
        const wildfireProjection = {};
        const wildfireDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const wildfireDbName = "arcgis";
        const wildfireCollectionName = "map";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "training";
        loadTransform(wildfireQuery, wildfireProjection, wildfireDbUrl, wildfireDbName, wildfireCollectionName, (transformError) => {
            if (transformError) {
                reject(transformError);
            } else {
                resolve();
            }
        }, (docs, loadTransformCallback) => {
            const wildfireMap = docs[0];
            streamSave(climateQuery, climateProjection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
                if (err) {
                    loadTransformCallback(err);
                } else {
                    loadTransformCallback();
                }
            }, (doc, cb) => {
                const res = {};
                res["Event"] = doc["Event"];
                res["Latitude"] = doc["Latitude"];
                res["Longitude"] = doc["Longitude"];
                // populate climate feature columns
                // TODO: Break this out into a function and test further
                for (let j = 0; j < doc.points.length; j++) {
                    const point = doc.points[j];
                    const label = j - COMBINE_CONFIG.units < 0 ?
                        `_${COMBINE_CONFIG.units - j}` : j - COMBINE_CONFIG.units;
                    // choose which column names to exclude
                    const props = COMBINE_CONFIG.props ? COMBINE_CONFIG.props : Object.keys(point).filter(k => k != "time" && k != "icon");
                    for (let k = 0; k < props.length; k++) {
                        const prop = props[k];
                        res[`${prop}${label}`] = point[prop];
                    }
                }
                // set other features
                res["Size"] = wildfireMap[doc["Event"]]["Size"];
                res["Costs"] = wildfireMap[doc["Event"]]["Costs"];
                cb(null, res);
                console.log('finished transforming doc');
            });
        });
    });
}

/**
 * Creates the training.trainingReduced collection by expanding climate attributes
 * into columns with time indexed labels, keeping all documents where "temperature", "humidity", and "windSpeed" 
 * attributes exist, using the climate.training2 collection and arcgis.map collection.
 */
function createTrainingReduced() { // uses streams
    return new Promise((resolve, reject) => {
        const climateQuery = {};
        const climateProjection = {};

        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "training2";

        const wildfireQuery = {};
        const wildfireProjection = {};
        const wildfireDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const wildfireDbName = "arcgis";
        const wildfireCollectionName = "map";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "trainingReduced";
        loadTransform(wildfireQuery, wildfireProjection, wildfireDbUrl, wildfireDbName, wildfireCollectionName, (transformError) => {
            if (transformError) {
                reject(transformError);
            } else {
                resolve();
            }
        }, (docs, loadTransformCallback) => {
            const wildfireMap = docs[0];
            streamSave(climateQuery, climateProjection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
                if (err) {
                    loadTransformCallback(err);
                } else {
                    loadTransformCallback();
                }
            }, (doc, cb) => {
                const res = {};
                res["Event"] = doc["Event"];
                res["Latitude"] = doc["Latitude"];
                res["Longitude"] = doc["Longitude"];
                // populate climate feature columns
                // TODO: Break this into a function and test further
                for (let j = 0; j < doc.points.length; j++) {
                    const point = doc.points[j];
                    const label = j - COMBINE_CONFIG.units < 0 ?
                        `_${COMBINE_CONFIG.units - j}` : j - COMBINE_CONFIG.units;
                    // choose which column names to keep
                    const props = COMBINE_CONFIG.props ? COMBINE_CONFIG.props : Object.keys(point).filter(k => k == "temperature" || k == "humidity" || k == "windSpeed");
                    for (let k = 0; k < props.length; k++) {
                        const prop = props[k];
                        res[`${prop}${label}`] = point[prop];
                    }
                }
                // set other features
                res["Size"] = wildfireMap[doc["Event"]]["Size"];
                res["Costs"] = wildfireMap[doc["Event"]]["Costs"];
                cb(null, res);
                console.log('finished transforming doc');
            });
        });
    });
}

/**
 * Creates the training.trainingFormat2 collection from the climate.training2 collection
 */
function createTrainingTrainingFormat2() { // uses streams
    return new Promise((resolve, reject) => {
        const climateQuery = {};
        const projection = {};

        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "training2";

        const wildfireQuery = {};
        const wildfireProjection = {};
        const wildfireDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const wildfireDbName = "arcgis";
        const wildfireCollectionName = "map";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "trainingFormat2";
        loadTransform(wildfireQuery, wildfireProjection, wildfireDbUrl, wildfireDbName, wildfireCollectionName, (transformError) => {
            if (transformError) {
                reject(transformError);
            } else {
                resolve();
            }
        }, (docs, loadTransformCallback) => {
            const wildfireMap = docs[0];
            streamSave(climateQuery, projection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
                if (err) {
                    loadTransformCallback(err);
                } else {
                    loadTransformCallback();
                }
            }, (doc, cb) => {
                console.log(`processing event '${doc["Event"]}'`);
                console.log(JSON.stringify(_.omit(doc, "points"), null, 4));
                // pick out features from wildfire event to include
                const eventId = doc["Event"];
                const latitude = doc["Latitude"];
                const longitude = doc["Longitude"];
                const features = {};
                for (let k = 0; k < doc.points.length; k++) {
                    const point = doc.points[k];
                    const props = Object.keys(point).filter(k => k != "time" && k != "icon");
                    for (let m = 0; m < props.length; m++) {
                        const prop = props[m];
                        if (!features[prop]) {
                            features[prop] = [];
                        }
                        features[prop].push({
                            time: point["time"],
                            [prop]: point[prop]
                        });
                    }
                }
                const size = wildfireMap[doc["Event"]]["Size"];
                const costs = wildfireMap[doc["Event"]]["Costs"];
                const res = {
                    'Event': eventId,
                    'Latitude': latitude,
                    'Longitude': longitude,
                    'Features': features,
                    'Size': size,
                    'Costs': costs
                };
                cb(null, res);
                console.log('finished transforming doc');
            });
        });
    });
}

/**
 * Entry point for the training data combination stage.
 */
function combineStages() {
    return new Promise((resolve, reject) => {
        createTrainingTraining()
            .then(createTrainingReduced)
            .then(createTrainingTrainingFormat2)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
};

module.exports = {
    combineStages
}