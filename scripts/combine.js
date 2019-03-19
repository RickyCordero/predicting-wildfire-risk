const logger = require('./logger');

const async = require('async');
const MongoClient = require('mongodb').MongoClient;
const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
const { Transform } = require('stream');
const { COMBINE_CONFIG } = require('./config');
const { loadSave } = require('./db');

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
        points: []
    };
    let key;
    if (datum["hourly"]) {
        key = "hourly";
    } else if (datum["daily"]) {
        key = "daily";
    }
    const objects = datum[key];
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

const transformClimateDatumStream = (transformChunk) => new Transform({
    readableObjectMode: true, // pass an object
    writableObjectMode: true, // read an object
    transform(chunk, _encoding, callback) {
        const transformedDatum = transformChunk(chunk);
        this.push(transformedDatum);
        callback();
    }
});

function createClimateStreamView(query, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, cb, transform) {
    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "climate";
    MongoClient.connect(sourceDbUrl, (sourceDbError, sourceDbClient) => {
        if (sourceDbError) {
            logger.warn('yo, there was an error connecting to the local database');
            cb(sourceDbError);
        } else {
            MongoClient.connect(outputDbUrl, (outputDbError, outputDbClient) => {
                if (outputDbError) {
                    logger.warn('yo, there was an error connecting to the local database');
                    cb(outputDbError);
                } else {
                    const sourceDb = sourceDbClient.db(sourceDbName);
                    const sourceCollection = sourceDb.collection(sourceCollectionName);
                    const outputDb = outputDbClient.db(outputDbName);

                    const processedClimateOutputDbConfig = {
                        dbURL: "",
                        dbConnection: outputDb,
                        batchSize: 50,
                        collection: outputCollectionName
                    };

                    const readStream = sourceCollection.find(query).stream();
                    const writeStream = streamToMongoDB(processedClimateOutputDbConfig);

                    readStream
                        .pipe(transformClimateDatumStream(transform))
                        .pipe(writeStream)
                        .on('data', (chunk) => {
                            logger.info('processing chunk');
                        })
                        .on('error', (err) => {
                            logger.warn(`yo, there was an error writing to ${outputDbName}/${outputCollectionName}`);
                            cb(err);
                        })
                        .on('finish', () => {
                            logger.info('finished cleaning climate data');
                            sourceDbClient.close();
                            outputDbClient.close();
                            cb();
                        });
                }
            });
        }
    });
}

/**
 * Creates the climate.filteredPoints collection from the climate.training collection
 */
function createFilteredPoints() { // uses buffer
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "climate";
        const sourceCollectionName = "training";
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "climate";
        const outputCollectionName = "filteredPoints";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            cb(null, docs
                .map(transformClimateDatum)
                .filter((doc) => {
                    return doc.points.length == 696;
                }));
        });
    });
}

/**
 * Creates the training.training collection from the climate.filteredPoints collection
 */
function createWildfirePoints() { // uses buffer
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "climate";
        const sourceCollectionName = "filteredPoints";

        const wildfireDbUrl = "mongodb://localhost:27017";
        const wildfireDbName = "arcgis";
        const wildfireCollectionName = "training"

        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "training";
        const outputCollectionName = "training";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            MongoClient.connect(wildfireDbUrl, (wildfireDbError, wildfireDbClient) => {
                if (wildfireDbError) {
                    cb(wildfireDbError);
                } else {
                    const wildfireDb = wildfireDbClient.db(wildfireDbName);
                    const wildfireCollection = wildfireDb.collection(wildfireCollectionName);
                    wildfireCollection.find({}).toArray((queryError, queryResults) => {
                        if (queryError) {
                            mapCb(queryError);
                        } else {
                            async.map(docs, (doc, mapCb) => {
                                const eventId = doc["Event"];
                                const event = queryResults.find(q => q["Event"] == eventId);
                                if (event) {
                                    logger.info(`found event '${eventId}'`);
                                    const size = event["Size"];
                                    const costs = event["Costs"];
                                    const features = doc.points
                                        .map((datum, idx) => {
                                            const label = idx - COMBINE_CONFIG.units < 0 ?
                                                `_${COMBINE_CONFIG.units - idx}` : idx - COMBINE_CONFIG.units;
                                            return COMBINE_CONFIG.props.reduce((acc, prop) => {
                                                return ({ ...acc, [`${prop}${label}`]: datum[prop] })
                                            }, {});
                                        })
                                        .reduce((acc, obj) => ({ ...acc, ...obj }), {});
                                    mapCb(null, {
                                        'Event': eventId,
                                        ...features,
                                        'Size': size,
                                        'Costs': costs
                                    });
                                } else {
                                    mapCb('no event found in wildfire db');
                                }
                            }, (mapError, mapResult) => {
                                if (mapError) {
                                    cb(mapError);
                                } else {
                                    cb(null, mapResult);
                                }
                                wildfireDbClient.close();
                                logger.info('finished map');
                            });
                        }
                    });
                }
            });
        });
    });
}

/**
 * Creates the remote training.training collection from the local training.training collection
 */
function uploadWildfirePoints() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "training";
        const sourceCollectionName = "training";
        const outputDbUrl = process.env.MONGODB_URL;
        const outputDbName = "training";
        const outputCollectionName = "training";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err, res) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Entry point for the training data combination stage.
 */
function combineData() {
    return new Promise((resolve, reject) => {
        createFilteredPoints()
            .then(createWildfirePoints)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
};

module.exports = {
    combineData
}