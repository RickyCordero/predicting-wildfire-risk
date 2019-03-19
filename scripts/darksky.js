const logger = require('./logger');

const request = require('request');
const moment = require('moment-timezone');

const { Transform } = require('stream');

const INTERVALS = ['currently', 'minutely', 'hourly', 'alerts', 'flags', 'daily'];

/**
 * Retrieves and returns historical weather data for each wildfire object in the wildfires array from the given config object
 * @param {Object} config - A configuration object containing the following properties:
 *  * @param {String} apiKey - A Dark Sky API key
 *  * @param {String} interval - A string representing the level of time granularity to include in each request object
 *  * @param {Number} units - The number of intervals of data to collect after each wildfire's ignition time
 *  * @param {Number} limit - The max number of requests to process at once
 *  * @param {Number} max - The max number of requests to make in total
 *  * @param {Array<Object>} wildfires - An array of wildfire objects
 * @param {Function} cb - The next function to call
 */
const getHistoricalClimateData = (config, cb) => {
    async.map(config.wildfires, (wildfire, mapCallback) => {
        getHistoricalClimateDataEach({ ...config, "wildfire": wildfire }, mapCallback);
    }, (mapError, mapResult) => {
        if (mapError) {
            cb(mapError);
        } else {
            cb(null, mapResult);
        }
    });
};

/**
 * Retrieves and returns historical weather data for a wildfire event object in the given config object
 * @param {Object} config - A configuration object containing the following properties:
 *  * @param {String} apiKey - A Dark Sky API key
 *  * @param {String} interval - A string representing the level of time granularity to include in each request object
 *  * @param {Number} units - The number of intervals of data to collect after each wildfire's ignition time
 *  * @param {Number} limit - The max number of requests to process at once for a single wildfire event
 *  * @param {Object} wildfire - A wildfire event object
 * @param {Function} cb - The next function to call
 */
const getHistoricalClimateDataEach = (config, cb) => {
    const wildfire = config.wildfire;
    const eventId = wildfire["Event"];
    logger.info(`collecting climate data for event ${eventId}`);
    if (wildfire["Latitude"] != null && wildfire["Longitude"] != null && wildfire["Start Date"] != null) {
        const latitude = wildfire["Latitude"];
        const longitude = wildfire["Longitude"];
        const ignitionDate = wildfire["Start Date"];
        // construct window bounds
        if (config.interval == 'daily' || config.interval == 'hourly') {
            let startDate;
            let endDate;
            // convert date strings back to moment objects with offset
            if (config.interval == 'daily') {
                startDate = moment.parseZone(ignitionDate).subtract(config.units, 'days');
                endDate = moment.parseZone(ignitionDate).add(config.units, 'days');
            } else if (config.interval == 'hourly') {
                startDate = moment.parseZone(ignitionDate).subtract(config.units, 'hours');
                endDate = moment.parseZone(ignitionDate).add(config.units, 'hours');
            }
            if (startDate.isValid() && endDate.isValid()) {
                if (startDate.isSameOrBefore(endDate)) {
                    getClimateDataInRange({ ...config, startDate, endDate, latitude, longitude }, (climateError, climateResult) => {
                        if (climateError) {
                            cb(climateError);
                        } else {
                            climateResult["Event"] = eventId;
                            cb(null, climateResult);
                        }
                    });
                } else {
                    const dateOrderError = { error: `start date must be earlier than end date for event ${eventId}, (${startDate}, ${endDate})` };
                    logger.warn(dateOrderError.error);
                    cb(dateOrderError);
                }
            } else {
                const dateError = { error: `start date or end date is invalid for event ${eventId}, (${startDate}, ${endDate})` };
                logger.warn(dateError.error);
                cb(dateError);
            }
        } else {
            const timeError = { error: `unsupported time interval requested for event ${eventId}, (${config.interval})` };
            logger.warn(timeError.error);
            cb(timeError);
        }
    } else {
        const propertyError = { error: `latitude, longitude, or start date not provided for event ${eventId}, (${latitude},${longitude},${startDate})` };
        logger.warn(propertyError.error);
        cb(propertyError);
    }
};

/**
 * Entry point for downloading historical weather data for a given geolocation and time interval using the Dark Sky Time Machine API
 * @param {Object} config - A configuration object containing the following properties:
 *  * @param {String} apiKey - A Dark Sky API key
 *  * @param {String} interval - A string representing the level of time granularity to include in each request object
 *  * @param {Date} start - The earliest date for which climate data should be retrieved
 *  * @param {Date} end - The latest date for which climate data should be retrieved
 *  * @param {Number} latitude - The latitude of the requested location
 *  * @param {Number} longitude - The longitude of the requested location
 *  * @param {Number} units - The number of intervals of data to collect after each wildfire's ignition time
 *  * @param {Number} limit - The max number of requests to process at once
 * @param {Function} cb - The next function
 */
function getClimateDataInRange(config, cb) {
    // initialize results object
    const results = {
        [config.interval]: [],
        requests: 0,
        startDate: config.startDate.format(),
        endDate: config.endDate.format(),
        latitude: config.latitude,
        longitude: config.longitude
    };
    // choose the time intervals for which data should be requested
    const excludedIntervals = INTERVALS.filter(x => config.interval != x).join();
    // start the data accumulation process on the results object
    download(results, config.apiKey, config.startDate, config.endDate, config.latitude, config.longitude, config.interval, excludedIntervals, (downloadError, downloadResult) => {
        if (downloadError) {
            cb(downloadError);
        } else {
            cb(null, downloadResult);
        }
    });
}

/**
 * Recursively downloads historical weather data for a given geolocation and time interval using the Dark Sky API
 * @param {Object} results - The results object for which data should be accumulated into
 * @param {String} apiKey - A Dark Sky api key
 * @param {Date} startDate - The earliest date for which weather data should be retrieved
 * @param {Date} endDate - The latest date for which weather data should be retrieved
 * @param {Number} latitude - The latitude of the requested location
 * @param {Number} longitude - The longitude of the requested location
 * @param {String} interval - A string representing the time granularity to include per request
 * @param {String} excludedIntervals - The string of blocks to not include per request
 * @param {Function} cb - The next function to be called
 */
function download(results, apiKey, startDate, endDate, latitude, longitude, interval, excludedIntervals, cb) {
    const time = startDate.format();
    const url = `https://api.darksky.net/forecast/${apiKey}/${latitude},${longitude},${time}?exclude=${excludedIntervals}`;
    request(url, (requestError, _res, body) => {
        results.requests += 1;
        if (requestError) {
            logger.warn('yo, there was an error in the dark sky api request');
            cb({ error: requestError });
        } else {
            const obj = JSON.parse(body);
            results[interval].push(obj);
            if (startDate.isSameOrAfter(endDate)) {
                // base case: date equal or overshot => return results
                cb(null, results);
            } else {
                // otherwise, download data for the next day
                const newStartDate = startDate.clone().add(1, 'days');
                download(results, apiKey, newStartDate, endDate, latitude, longitude, interval, excludedIntervals, cb);
            }
        }
    });
}

const getClimateData = (climateConfig) => new Transform({
    writableObjectMode: true, // read an object
    readableObjectMode: true, // pass an object
    transform(chunk, _encoding, callback) {
        const obj = { ...climateConfig, wildfires: chunk };
        getHistoricalClimateData(obj, (err, res) => {
            if (err) {
                logger.debug(JSON.stringify(err, null, 4));
                this.push(err);
            } else {
                this.push(res);
            }
            callback();
        });
    }
});

const getClimateDataEach = (climateConfig) => new Transform({
    writableObjectMode: true, // read an object
    readableObjectMode: true, // pass an object
    transform(chunk, _encoding, callback) {
        logger.info(`processing event ${chunk["Event"]}`);
        const obj = { ...climateConfig, wildfire: chunk };
        getHistoricalClimateDataEach(obj, (err, res) => {
            // push errors and results through stream
            if (err) {
                logger.debug(JSON.stringify(err, null, 4));
                this.push(err);
            } else {
                this.push(res);
            }
            callback();
        });
    }
});

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

module.exports = {
    saveClimateData
}