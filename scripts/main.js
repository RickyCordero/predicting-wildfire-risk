const logger = require('./logger');

const async = require('async');

const { createUniqueView, createWildfireView, createStandardizedView } = require('./arcgis');
const { getClimateDataEach } = require('./darksky');
const { saveToDBEach, loadSave } = require('./db');
const { inBounds } = require('./utils');

const MongoClient = require('mongodb').MongoClient;
const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
const streamify = require('stream-array');

const DARKSKY_API_KEY = process.env.DARKSKY_API_KEY;

const CLIMATE_CONFIG = {
    apiKey: DARKSKY_API_KEY,
    interval: 'hourly', // can be 'daily' or 'hourly'
    units: 336, // number of units of interval data to collect before each fire's ignition time and after, not including ignition date (336 hours = 14 days * 24 hours)
    limit: 5 // max number of wildfire events to process in parallel
};

const COMBINE_CONFIG = {
    interval: CLIMATE_CONFIG.interval,
    units: CLIMATE_CONFIG.units,
    props: ['temperature', 'windSpeed', 'humidity'] // 'temperature' not available for 'daily' interval
};

/**
 * Creates the local arcgis/training collection from the local arcgis/standardized collection
 */
function createTrainingWildfires() {
    return new Promise((resolve, reject) => {
        const query = { $and: [{ "State": { $eq: "CA" } }, { $or: [{ "Size": { $gte: 0 } }, { "Costs": { $gte: 0 } }] }] };
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "arcgis";
        const outputCollectionName = "training";
        createStandardizedView(query, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs) => {
            return docs.filter(doc => {
                const inBoundsDoc = inBounds(doc["Latitude"], doc["Longitude"]);
                if (!inBoundsDoc) {
                    logger.warn(`event '${doc["Event"]}' not in bounds, (${doc["Latitude"]}, ${doc["Longitude"]})`);
                }
                return inBoundsDoc;
            });
        });
    });
}

/**
 * Creates a view of trainable standardized unique wildfire events
 * @param {Object} query - The mongo query object to be used for filtering
 * @param {String} outputDbUrl - The mongo url of the destination database
 * @param {String} outputDbName - The destination database name
 * @param {String} outputCollectionName - The destination collection name
 * @param {Function} callback - The next function to be called
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createTrainingView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "arcgis";
    const sourceCollectionName = "training";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Creates the local climate/training collection from the local arcgis/training collection
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

/**
 * Downloads and streams climate data for each wildfire from the given source collection into the output collection
 * @param {Object} query - The mongo query object
 * @param {String} sourceDbUrl - The mongo url of the source database
 * @param {String} sourceDbName - The name of the source database
 * @param {String} sourceCollectionName - The name of the source collection
 * @param {String} outputDbUrl - The mongo url of the output database
 * @param {String} outputDbName - The name of the output database
 * @param {String} outputCollectionName - The name of the output collection
 * @param {Function} callback - The next function to call
 */
const saveClimateData = (query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback) => {
    MongoClient.connect(sourceDbUrl, (sourceDbError, sourceDbClient) => {
        if (sourceDbError) {
            logger.warn('yo, there was an error connecting to the source database');
            callback(sourceDbError);
        } else {
            MongoClient.connect(outputDbUrl, (outputDbError, outputDbClient) => {
                if (outputDbError) {
                    logger.warn('yo, there was an error connecting to the output database');
                    callback(outputDbError);
                } else {
                    logger.info('connected to the source and output databases successfully');
                    const sourceDb = sourceDbClient.db(sourceDbName);
                    const sourceCollection = sourceDb.collection(sourceCollectionName);
                    const outputDb = outputDbClient.db(outputDbName);
                    
                    const climateOutputDbConfig = {
                        dbName: outputDbName,
                        dbURL: outputDbUrl, // unused
                        dbConnection: outputDb,
                        batchSize: 50,
                        collection: outputCollectionName
                    };
                    sourceCollection.find(query).toArray((queryError, queryResults) => {
                        if (queryError) {
                            callback(queryError);
                        } else {
                            async.mapLimit(queryResults, CLIMATE_CONFIG.limit, (item, mapCb) => {
                                const readStream = streamify([item]);
                                readStream
                                    .pipe(getClimateDataEach(CLIMATE_CONFIG))
                                    .pipe(saveToDBEach(climateOutputDbConfig))
                                    .on('error', (err) => {
                                        mapCb(err);
                                    })
                                    .on('finish', () => {
                                        logger.info(`done processing event ${item["Event"]}`);
                                        mapCb();
                                    });
                            }, (mapErr, _mapRes) => {
                                if (mapErr) {
                                    callback(mapErr);
                                } else {
                                    callback();
                                }
                                outputDbClient.close();
                                sourceDbClient.close();
                            });
                        }
                    });
                }
            });
        }
    });
};

/**
 * Creates the remote climate/training collection from the local climate/training collection using streams
 */
function uploadClimateData() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "climate";
        const sourceCollectionName = "training";
        const outputDbUrl = process.env.MONGODB_URL;
        const outputDbName = "climate";
        const outputCollectionName = "training";
        MongoClient.connect(sourceDbUrl, (sourceDbError, sourceDbClient) => {
            if (sourceDbError) {
                reject(sourceDbError);
            } else {
                MongoClient.connect(outputDbUrl, (outputDbError, outputDbClient) => {
                    if (outputDbError) {
                        reject(outputDbError);
                    } else {
                        const sourceDb = sourceDbClient.db(sourceDbName);
                        const sourceCollection = sourceDb.collection(sourceCollectionName);
                        const outputDb = outputDbClient.db(outputDbName);
                        const _outputCollection = outputDb.collection(outputCollectionName);

                        const climateOutputDbConfig = {
                            dbURL: outputDbUrl, // unused
                            dbConnection: outputDb,
                            batchSize: 50,
                            collection: outputCollectionName
                        };

                        const writableStream = streamToMongoDB(climateOutputDbConfig);

                        const readStream = sourceCollection.find(query).stream();

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
                                sourceDbClient.close();
                                outputDbClient.close();
                                resolve();
                            });
                    }
                })
            }
        });
    });
}
