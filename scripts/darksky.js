const logger = require('./logger');

const async = require('async');
const request = require('request');
const { Transform } = require('stream');
const MongoClient = require('mongodb').MongoClient;
const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
const streamify = require('stream-array');
const moment = require('moment-timezone');

const { WILDFIRE_CONFIG } = require('./config');
const { CLIMATE_CONFIG } = require('./config');
const { saveToDBEach } = require('./db');

const INTERVALS = ['currently', 'minutely', 'hourly', 'alerts', 'flags', 'daily'];


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
 * Transforms a climate data object.
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
 *  * @param {Object} event - An event object
 * @param {Function} cb - The next function to call
 */
const getHistoricalClimateDataEach = (config, cb) => {
    const event = config.event;
    const eventId = event["Event"];
    logger.info(`collecting climate data for event ${eventId}`);
    if (event["Latitude"] != null && event["Longitude"] != null && event["Start Date"] != null) {
        const latitude = event["Latitude"];
        const longitude = event["Longitude"];
        const ignitionDate = event["Start Date"];
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
            let obj = {};
            try {
                obj = JSON.parse(body);
            } catch (err) {
                logger.debug(err);
                logger.debug(body);
            }
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

/**
 * Creates a transform stream object to collect climate
 * data for an array of wildfire events.
 * @param {Object} climateConfig - The config object
 */
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

/**
 * Creates a transform stream object to collect climate
 * data for a given wildfire event.
 * @param {Object} climateConfig - The config object
 */
const getClimateDataEach = (climateConfig) => new Transform({
    writableObjectMode: true, // read an object
    readableObjectMode: true, // pass an object
    transform(chunk, _encoding, callback) {
        logger.info(`processing event`);
        const obj = { ...climateConfig, event: chunk };
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
 * Downloads and streams climate data for each wildfire from the given source collection into the output collection.
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
                                        logger.info(`done processing event`);
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
 * Creates the climate.training collection from the arcgis.training collection.
 */
function saveClimateDataFromArcgis() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = WILDFIRE_CONFIG.PRIMARY_DB_NAME;
        const sourceCollectionName = "training";
        // TODO: Move the output climate database info to the climate config object
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
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
 * Creates the climate.training2 collection by restructuring each 
 * climate data object's climate data attributes for each object
 * in the climate.training collection.
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
 * Creates the climate.map collection from the climate.training collection.
 */
function createTrainingClimateMap() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "training";
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "climate";
        const outputCollectionName = "map";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            // splits the climate training collection into partitions
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


/**
 * Creates the remote climate.training collection from the local climate/training collection using streams.
 */
function backupClimateData() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "climate";
        const sourceCollectionName = "training";
        // TODO: Move the remote output climate database info to the climate config object
        const outputDbUrl = WILDFIRE_CONFIG.SECONDARY_MONGODB_URL;
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

/**
 * Entry point for the climate data processing stage.
 */
function climateStages() {
    return new Promise((resolve, reject) => {
        saveClimateDataFromArcgis()
            .then(createClimateTraining2)
            .then(createTrainingClimateMap)
            .then(backupClimateData)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
}

module.exports = {
    climateStages
}