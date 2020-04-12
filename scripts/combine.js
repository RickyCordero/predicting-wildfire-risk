const logger = require('./logger');

const _ = require('lodash');
const async = require('async');
const MongoClient = require('mongodb').MongoClient;
const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
const { Transform } = require('stream');
const { COMBINE_CONFIG } = require('./config');
const { loadSave, loadTransform, streamSave, streamTransform } = require('./db');
const { binarySearch } = require('./utils');


/*
    Sample Dark Sky Time Machine response object at the 'daily' interval (with time transformation):

            https://darksky.net/dev/docs#data-point-object
            {
                "time": "2007-07-05T07:00:00.000Z",
                "summary": "Mostly cloudy throughout the day.",
                "icon": "partly-cloudy-day",
                "sunriseTime": 1183640022,
                "sunsetTime": 1183691908,
                "moonPhase": 0.68,
                "precipIntensity": 0,
                "precipIntensityMax": 0,
                "precipProbability": 0,
                "temperatureHigh": 85.11,
                "temperatureHighTime": 1183669200,
                "temperatureLow": 50.99,
                "temperatureLowTime": 1183716000,
                "apparentTemperatureHigh": 85.11,
                "apparentTemperatureHighTime": 1183669200,
                "apparentTemperatureLow": 50.99,
                "apparentTemperatureLowTime": 1183716000,
                "dewPoint": 51.24,
                "humidity": 0.64,
                "pressure": 1011.77,
                "windSpeed": 1.91,
                "windGust": 13.12,
                "windGustTime": 1183690800,
                "windBearing": 260,
                "cloudCover": 0.63,
                "uvIndex": 10,
                "uvIndexTime": 1183665600,
                "visibility": 6.51,
                "temperatureMin": 55.28,
                "temperatureMinTime": 1183701600,
                "temperatureMax": 85.11,
                "temperatureMaxTime": 1183669200,
                "apparentTemperatureMin": 55.28,
                "apparentTemperatureMinTime": 1183701600,
                "apparentTemperatureMax": 85.11,
                "apparentTemperatureMaxTime": 1183669200
            }
*/


/**
 * Transforms a climate data object
 * @param {Object} datum - The climate datum object
 */
function transformClimateDatum(datum) {
    /**
     * Example schema of a climate.training document
     * {
     *      hourly: [ // 29 items
     *          {
     *              latitude: 33.6172,
     *              longitude: -116.15083
     *              timezone: "America/Los_Angeles"
     *              hourly: {
     *                  summary: "Mostly cloudy until morning and breezy starting in the afternoon",
     *                  icon: "wind",
     *                  data: [ // 24 items
     *                          {
     *                            time: 1011772800,
     *                            summary: "Overcast",
     *                            icon: "cloudy",
     *                            precipType: "rain",
     *                            temperature: 55.44,
     *                            apparentTemperature: 55.44,
     *                            dewPoint: 20.01,
     *                            humidity: 0.25,
     *                            pressure: 1014.73,
     *                            windSpeed: 7.88,
     *                            windGust: 11.51,
     *                            windBearing: 342,
     *                            cloudCover: 1,
     *                            uvIndex: 0,
     *                            visibility: 10
     *                          },
     *                          ...
     *                  ]
     *              },
     *              offset: -8
     *          },
     *          ...
     *      ],
     *      requests: 29,
     *      startDate: "2002-01-23T00:00:00-08:00"
     *      endDate: "2002-02-20T00:00:00-08:00",
     *      latitude: -116.15083,
     *      Event: "CA-RRU-009418"
     * }
     */
    const res = {
        "Event": datum["Event"],
        "Latitude": datum["latitude"],
        "Longitude": datum["longitude"],
        points: []
    };
    let key;
    if (datum["hourly"]) {
        key = "hourly";
    } else if (datum["daily"]) {
        key = "daily";
    }
    const objects = datum[key];
    if (objects.length == 1) {
        logger.warn("I think I found a missing event:")
        logger.warn(datum["Event"]);
    }
    if (objects) {
        res.points = objects.reduce((points, dayObj) => {
            if (dayObj[key]) {
                if (dayObj[key].data) {
                    return points.concat(dayObj[key].data);
                }
            }
            return points;
        }, []);
    }
    return res;
}


/**
 * Creates the climate.training2 collection from the climate.training collection
 */
function createClimateTraining2() { // uses streams
    return new Promise((resolve, reject) => {
        const query = {};
        const projection = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "training";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "climate";
        const outputCollectionName = "training2";
        streamSave(query, projection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (doc, cb) => {
            if (doc.error) {
                logger.warn(`filtering out error found in ${sourceDbName}/${sourceCollectionName}`);
                cb(doc.error);
            } else {
                cb(null, transformClimateDatum(doc));
            }
        });
    });
}

/**
 * Creates the training.training collection from the climate.training2 collection
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
                for (let j = 0; j < doc.points.length; j++) {
                    const point = doc.points[j];
                    const label = j - COMBINE_CONFIG.units < 0 ?
                        `_${COMBINE_CONFIG.units - j}` : j - COMBINE_CONFIG.units;
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
 * Creates the training.trainingReduced collection from the climate.training2 collection
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
                for (let j = 0; j < doc.points.length; j++) {
                    const point = doc.points[j];
                    const label = j - COMBINE_CONFIG.units < 0 ?
                        `_${COMBINE_CONFIG.units - j}` : j - COMBINE_CONFIG.units;
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
 * Creates the climate.nonfire2 collection from the climate.nonfire collection
 */
function createClimateNonFire2() { // uses streams
    return new Promise((resolve, reject) => {
        const query = {};
        const projection = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "nonfire";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "climate";
        const outputCollectionName = "nonfire2";
        streamSave(query, projection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (doc, cb) => {
            if (doc.error) {
                logger.warn(`filtering out error found in ${sourceDbName}/${sourceCollectionName}`);
            }
            cb(null, transformClimateDatum(doc));
        });
    });
}



/**
 * Creates the training.nonfire collection from the climate.nonfire2 collection
 */
function createTrainingNonFire() { // uses streams
    return new Promise((resolve, reject) => {
        const query = {};
        const projection = {};

        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "nonfire2";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "nonfire";
        streamSave(query, projection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (doc, cb) => {

            console.log(`processing event '${doc["Event"]}'`);
            console.log(JSON.stringify(_.omit(doc, "points"), null, 4));

            const res = {};
            res["Event"] = doc["Event"];
            res["Latitude"] = doc["Latitude"];
            res["Longitude"] = doc["Longitude"];
            // populate climate feature columns
            for (let j = 0; j < doc.points.length; j++) {
                const point = doc.points[j];
                const label = j - COMBINE_CONFIG.units < 0 ?
                    `_${COMBINE_CONFIG.units - j}` : j - COMBINE_CONFIG.units;
                const props = COMBINE_CONFIG.props ? COMBINE_CONFIG.props : Object.keys(point).filter(k => k != "time" && k != "icon");
                for (let k = 0; k < props.length; k++) {
                    const prop = props[k];
                    res[`${prop}${label}`] = point[prop];
                }
            }
            // set other features
            res["Size"] = 0;
            res["Costs"] = 0;
            console.log('finished transforming doc');
            cb(null, res);
        });
    });
}

/**
 * Creates the training.nonfireReduced collection from the climate.nonfire2 collection
 */
function createTrainingNonFireReduced() { // uses streams
    return new Promise((resolve, reject) => {
        const query = {};
        const projection = {};

        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "nonfire2";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "nonfireReduced";
        streamSave(query, projection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (doc, cb) => { // function to transform each doc from source collection before writing to destination

            console.log(`processing event '${doc["Event"]}'`);
            console.log(JSON.stringify(_.omit(doc, "points"), null, 4));

            const res = {};
            res["Event"] = doc["Event"];
            res["Latitude"] = doc["Latitude"];
            res["Longitude"] = doc["Longitude"];
            // populate climate feature columns
            for (let j = 0; j < doc.points.length; j++) {
                const point = doc.points[j];
                const label = j - COMBINE_CONFIG.units < 0 ?
                    `_${COMBINE_CONFIG.units - j}` : j - COMBINE_CONFIG.units;
                const props = COMBINE_CONFIG.props ? COMBINE_CONFIG.props : Object.keys(point).filter(k => k == "temperature" || k == "humidity" || k == "windSpeed");
                for (let k = 0; k < props.length; k++) {
                    const prop = props[k];
                    res[`${prop}${label}`] = point[prop];
                }
            }
            // set other features
            res["Size"] = 0;
            res["Costs"] = 0;
            console.log('finished transforming doc');
            cb(null, res);
        });
    });
}

/**
 * Creates the training.nonfireFormat2 collection from the climate.nonfire2 collection
 */
function createTrainingNonFireFormat2() { // uses streams
    return new Promise((resolve, reject) => {
        const query = {};
        const projection = {};

        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "nonfire2";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "nonfireFormat2";
        streamSave(query, projection, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
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
            const size = 0;
            const costs = 0;
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
}

/**
 * Creates the training.firewithnonfire
 */
function createFireWithNonfire() {
    return new Promise((resolve, reject) => {
        const query_1 = {};
        const projection_1 = {};

        const query_2 = {};
        const projection_2 = {};

        const sourceDbUrl_1 = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName_1 = "training";
        const sourceCollectionName_1 = "training";

        const sourceDbUrl_2 = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName_2 = "training";
        const sourceCollectionName_2 = "nonfire";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "firewithnonfire";

        // stream the documents from the first collection to the output database
        streamSave(query_1, projection_1, sourceDbUrl_1, sourceDbName_1, sourceCollectionName_1, outputDbUrl, outputDbName, outputCollectionName, (err_1) => {
            if (err_1) {
                reject(err_1);
            } else {
                // stream the documents from the second collection to the output database
                streamSave(query_2, projection_2, sourceDbUrl_2, sourceDbName_2, sourceCollectionName_2, outputDbUrl, outputDbName, outputCollectionName, (err_2) => {
                    if (err_2) {
                        reject(err_2);
                    } else {
                        resolve();
                    }
                }, (doc, cb) => {
                    cb(null, doc);
                });
            }
        }, (doc, cb) => {
            cb(null, doc);
        });
    });
}

/**
 * Creates the training.firewithnonfire
 */
function createFireWithNonfireReduced() {
    return new Promise((resolve, reject) => {
        const query_1 = {};
        const projection_1 = {};

        const query_2 = {};
        const projection_2 = {};

        const sourceDbUrl_1 = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName_1 = "training";
        const sourceCollectionName_1 = "trainingReduced";

        const sourceDbUrl_2 = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName_2 = "training";
        const sourceCollectionName_2 = "nonfireReduced";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "firewithnonfireReduced";

        // stream the documents from the first collection to the output database
        streamSave(query_1, projection_1, sourceDbUrl_1, sourceDbName_1, sourceCollectionName_1, outputDbUrl, outputDbName, outputCollectionName, (err_1) => {
            if (err_1) {
                reject(err_1);
            } else {
                // stream the documents from the second collection to the output database
                streamSave(query_2, projection_2, sourceDbUrl_2, sourceDbName_2, sourceCollectionName_2, outputDbUrl, outputDbName, outputCollectionName, (err_2) => {
                    if (err_2) {
                        reject(err_2);
                    } else {
                        resolve();
                    }
                }, (doc, cb) => {
                    cb(null, doc);
                });
            }
        }, (doc, cb) => {
            cb(null, doc);
        });
    });
}

/**
 * Creates the training.firewithnonfireReducedFiltered
 */
function createFireWithNonfireReducedFiltered() {
    return new Promise((resolve, reject) => {
        const query_1 = {};
        const projection_1 = {};

        const query_2 = {};
        const projection_2 = {};

        const sourceDbUrl_1 = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName_1 = "training";
        const sourceCollectionName_1 = "trainingReducedFiltered";

        const sourceDbUrl_2 = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName_2 = "training";
        const sourceCollectionName_2 = "nonfireReduced";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "firewithnonfireReducedFiltered";

        // stream the documents from the first source collection to the output collection
        streamSave(query_1, projection_1, sourceDbUrl_1, sourceDbName_1, sourceCollectionName_1, outputDbUrl, outputDbName, outputCollectionName, (err_1) => {
            if (err_1) {
                reject(err_1);
            } else {
                // stream the documents from the second source collection to the output database
                streamSave(query_2, projection_2, sourceDbUrl_2, sourceDbName_2, sourceCollectionName_2, outputDbUrl, outputDbName, outputCollectionName, (err_2) => {
                    if (err_2) {
                        reject(err_2);
                    } else {
                        resolve();
                    }
                }, (doc, cb) => {
                    cb(null, doc);
                });
            }
        }, (doc, cb) => {
            cb(null, doc);
        });
    });
}

/**
 * Entry point for the training data combination stage.
 */
function combineStages() {
    return new Promise((resolve, reject) => {
        createClimateTraining2()
            .then(createWildfirePoints)
            .then(combineFireWithNonfire)
            .then(createFireWithNonfireReducedFiltered)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
};

module.exports = {
    combineStages
}