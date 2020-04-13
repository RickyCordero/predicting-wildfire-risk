const logger = require('./logger');

const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
const streamify = require('stream-array');

const { WILDFIRE_CONFIG } = require('./config');

/**
 * Reads a JSON file containing nonfire event data.
 */
function loadNonFireJSON() {
    return new Promise((resolve, reject) => {
        const content = fs.readFileSync(".\\data\\non-fire-events.json");
        const json = JSON.parse(content);
        console.log(typeof (json));
        console.log(JSON.stringify(json, null, 4));
        resolve(json);
    });
}

/**
 * Creates the arcgis.nonfire collection by loading the nonfire JSON.
 */
function createNonFireEvents() {
    return new Promise((resolve, reject) => {
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = WILDFIRE_CONFIG.PRIMARY_DB_NAME;
        const outputCollectionName = "nonfire";
        MongoClient.connect(outputDbUrl, (outputDbError, outputDbClient) => {
            if (outputDbError) {
                reject(outputDbError);
            } else {
                const outputDb = outputDbClient.db(outputDbName);

                const nonfireOutputDbConfig = {
                    dbURL: outputDbUrl, // unused
                    dbConnection: outputDb,
                    batchSize: 50,
                    collection: outputCollectionName
                };

                const writableStream = streamToMongoDB(nonfireOutputDbConfig);

                loadNonFireJSON()
                    .then(json => {
                        // Add "Event" field to each nonfire event
                        json = json.map((event, idx) => ({ ...event, "Event": `NONFIRE_${idx}` }));
                        // Create read stream for each json object
                        const readStream = streamify(json);
                        readStream
                            .pipe(writableStream)
                            .on('data', (chunk) => {
                                logger.info(`processing chunk`);
                            })
                            .on('error', (err) => {
                                logger.warn(`yo, there was an error writing to ${outputDbName}/${outputCollectionName}`);
                                reject(err);
                            })
                            .on('finish', () => {
                                logger.info(`saved data to ${outputDbName}/${outputCollectionName} successfully`);
                                outputDbClient.close();
                                resolve();
                            });
                    })
                    .catch(reject);
            }
        });
    });
}

/**
 * Creates the climate.nonfire collection from the arcgis.nonfire collection.
 */
function collectNonFireClimate() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "arcgis";
        const sourceCollectionName = "nonfire";

        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "climate";
        const outputCollectionName = "nonfire";
        saveClimateData(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                logger.warn('yo there was an error saving the nonfire climate data');
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Creates the climate.nonfire2 collection from the climate.nonfire collection.
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
 * Creates the training.nonfire collection from the climate.nonfire2 collection.
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
 * Creates the training.nonfireReduced collection from the climate.nonfire2 collection.
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
 * Creates the training.firewithnonfire collection by unioning all training
 * objects from the training.nonfire and training.firewithnonfire collections.
 */
function createTrainingFireWithNonfire() {
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

        // TODO: Figure out how to implement with Mongo directly

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
 * Creates the training.firewithnonfire collection.
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
 * Creates the training.firewithnonfireReducedFiltered collection.
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



function nonfireStages() {
    return new Promise((resolve, reject) => {
        createNonFireEvents()
            .then(collectNonFireClimate)
            .then(createClimateNonFire2)
            .then(createTrainingNonFire)
            .then(createTrainingNonFireFormat2)
            .then(createTrainingNonFireReduced)
            .then(createTrainingFireWithNonfire)
            .then(createFireWithNonfireReduced)
            .then(createFireWithNonfireReducedFiltered)
            .then(resolve)
            .catch((err) => {
                reject(err);
            });
    });
}

module.exports = {
    nonfireStages
}