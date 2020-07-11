const logger = require('./logger');

const async = require('async');
const request = require('request');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
const tzlookup = require('tz-lookup');
const moment = require('moment-timezone');

const { WILDFIRE_CONFIG } = require('./config');
const { saveToDBBuffer, loadSave } = require('./db');
const { dateComparator, filterMaxInfo } = require('./utils');
const { queryA, queryB, queryC } = require('./queries');

const FEATURES_A = ["startdate", "ename", "latdd", "longdd", "un_ustate", "acres", "ecosts", "event_id"];
const FEATURES_B = ["start_date", "start_hour", "fire_name", "latitude", "longitude", "state", "area_", "area_meas", "incident_n"];
const FEATURES_C = ["firediscoverydatetime", "incidentname", "latitude", "longitude", "state", "acres", "uniquefireidentifier"];

/**
 * Downloads raw wildfire data from the USGS ArcGIS REST API from 2002 to 2018
 * and saves it to the arcgis.raw collection
 * https://rmgsc-haws1.cr.usgs.gov/arcgis/sdk/rest/index.html#//02ss0000006v000000
 * id=27 => 2002
 * id=26 => 2003
 * ...
 * id=10 => 2019
 */
function downloadRaw() {
    return new Promise((resolve, reject) => {
        MongoClient.connect(WILDFIRE_CONFIG.PRIMARY_MONGODB_URL, (dbError, dbClient) => {
            if (dbError) {
                logger.warn('yo, there was an error connecting to the database');
                reject(dbError);
            } else {
                // generate the ids that correspond to a year
                const yearIds = [];
                for (let i = 10; i <= 27; i++) {
                    yearIds.push(i);
                }
                // query the API and get data for each year
                async.map(yearIds, (yearId, mapCb) => {
                    const url = `https://wildfire.cr.usgs.gov/ArcGIS/rest/services/geomac_dyn/MapServer/${yearId}/query?where=1%3D1&outFields=*&outSR=4326&f=json`;
                    request(url, (err, _res, body) => {
                        if (err) {
                            mapCb(err);
                        } else {
                            // parse the API response as a JSON object
                            const obj = JSON.parse(body);
                            mapCb(null, obj);
                        }
                    });
                }, (mapErr, mapRes) => {
                    if (mapErr) {
                        logger.warn('yo, there was a map error');
                        reject(mapErr);
                    } else {
                        // if successful, save all API response JSON objects to the "raw" collection
                        logger.info('finished downloading all data');
                        const dbName = "arcgis";
                        const collectionName = "raw";
                        saveToDBBuffer(dbClient.db(dbName), mapRes, collectionName, (err, _res) => {
                            if (err) {
                                logger.warn('yo, there was an error saving to the database');
                                reject(err);
                            } else {
                                logger.info(`successfully saved data to ${dbName}/${collectionName}`);
                                resolve();
                            }
                            dbClient.close();
                        });
                    }
                });
            }
        });
    });
}

/**
 * Creates the arcgis.events collection by constructing event objects 
 * using the raw response objects from the arcgis.raw collection.
 */
function createEventsFromRaw() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "arcgis";
        const sourceCollectionName = "raw";
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "arcgis";
        const outputCollectionName = "events";
        // Load each document from the "raw" collection
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            // Construct each event document from the response objects in the "raw" collection
            cb(null, docs.reduce((acc, obj) => {
                const rows = obj.features.map(f => f.attributes);
                return acc.concat(rows);
            }, []));
        });
    });
}

/**
 * Creates the arcgis.unique collection from the arcgis.events collection
 * by filtering out duplicate events.
 */
function createUniqueFromEvents() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "arcgis";
        const sourceCollectionName = "events";
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "arcgis";
        const outputCollectionName = "unique";
        // Load each document from the "raw" collection
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            cb(null, filterMaxInfo(docs, (doc) => {
                // Filter out duplicate documents from the "events" collection using this hash function
                if (doc["itype"]) {
                    return JSON.stringify(_.pick(doc, ["event_id"]));
                } else if (doc["inc_type"]) {
                    return JSON.stringify(_.pick(doc, ["incident_n"]));
                } else if (doc["incidenttypecategory"]) {
                    return JSON.stringify(_.pick(doc, ["uniquefireidentifier"]));
                }
                return JSON.stringify(doc); // shouldn't happen
            }));
        });
    });
}

/**
 * Creates a new collection by applying the given transformation function on
 * each event from the arcgis.unique collection of events.
 * @param {Object} query - The mongo query object
 * @param {String} outputDbUrl - The mongo url of the output database
 * @param {String} outputDbName - The name of the output database
 * @param {String} outputCollectionName - The name of the output collection
 * @param {Function} callback - The next function to call
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createUniqueView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
    const sourceDbName = "arcgis";
    const sourceCollectionName = "unique";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Creates the arcgis.wildfires collection by filtering out all non-wildfire 
 * events from the arcgis.unique collection.
 */
function createWildfiresFromUnique() {
    return new Promise((resolve, reject) => {
        // MongoDB query to identify only wildfire events
        const query = {
            $or: [
                queryA.isWildfire,
                queryB.isWildfire,
                queryC.isWildfire
            ]
        };
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "arcgis";
        const outputCollectionName = "wildfires";
        // Load query results into "wildfires" collection
        createUniqueView(query, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Creates a new collection by applying the given transformation function on 
 * each event from the arcgis.wildfire collection of wildfire events.
 * @param {Object} query - The mongo query object to be used for filtering
 * @param {String} outputDbUrl - The mongo url of the destination database
 * @param {String} outputDbName - The destination database name
 * @param {String} outputCollectionName - The destination collection name
 * @param {Function} callback - The next function to be called
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createWildfireView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
    const sourceDbName = "arcgis";
    const sourceCollectionName = "wildfires";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Creates the arcgis.standardized collection by applying a standardization function 
 * to each wildfire event document in the arcgis.wildfires collection.
 * Each document in the standardized collection represents a wildfire event.
 * Each event has attribute names uniformly described by "Event", "Incident Name", "Start Date", 
 * "Latitude", "Longitude", "State", "Size" and has values uniformly described in proper units.
 */
function createStandardizedWildfires() {
    return new Promise((resolve, reject) => {
        const query = {};
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "arcgis";
        const outputCollectionName = "standardized";
        // Load each document from the "wildfire" collection
        createWildfireView(query, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs) => {
            // Transform each document (can be of type A, B, or C) into a single standardized format
            const standardized = docs.map(doc => {
                if (doc["itype"]) { // 2002 - 2005
                    return processDocA(doc);
                }
                if (doc["inc_type"]) { // 2006 - 2015
                    return processDocB(doc);
                }
                if (doc["incidenttypecategory"]) { // 2016 - 2018
                    return processDocC(doc);
                }
            });
            return standardized.sort(dateComparator);
        });
    });
}

/**
 * Creates a new collection by applying the given transformation function on 
 * each event from the arcgis.standardized collection of wildfire events.
 * @param {Object} query - The mongo query object to be used for filtering
 * @param {String} outputDbUrl - The mongo url of the destination database
 * @param {String} outputDbName - The destination database name
 * @param {String} outputCollectionName - The destination collection name
 * @param {Function} callback - The next function to be called
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createStandardizedView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
    const sourceDbName = "arcgis";
    const sourceCollectionName = "standardized";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Given a raw wildfire event object of type A, returns a standardized representation of the object
 * that keeps all keys unused for climate gathering, but renames and/or removes keys used to create standardized keys.
 * @param {Object} doc - The wildfire event object
 */
function processDocA(doc) {
    const picked = _.pick(doc, FEATURES_A);
    return {
        ..._.omit(doc, FEATURES_A),
        "Event": picked["event_id"] ? picked["event_id"].trim() : null,
        "Incident Name": picked["ename"] ? picked["ename"].trim() : null,
        "Start Date": processDateA(picked["startdate"], picked["latdd"], picked["longdd"]),
        "Latitude": picked["latdd"] ? parseFloat(picked["latdd"]) : null,
        "Longitude": picked["longdd"] ? parseFloat(picked["longdd"]) : null,
        "State": picked["un_ustate"] ? picked["un_ustate"].trim() : null,
        "Size": picked["acres"] ? parseFloat(picked["acres"]) : null,
        "Costs": picked["ecosts"] ? parseFloat(picked["ecosts"]) : null
    };
}

/**
 * Creates a time zone adjusted string representation of the given start date at the given latitude
 * and longitude for a raw wildfire event object of type A
 * @param {String} startdate - The string representation of a start date in MM/DD/YYYY format
 * @param {Number} latitude - The floating point representation of a latitude coordinate
 * @param {Number} longitude - The floating point representation of a longitude coordinate
 */
function processDateA(startdate, latitude, longitude) {
    if (startdate != null && !isNaN(parseFloat(latitude)) && !isNaN(parseFloat(longitude))) {
        const timezone = tzlookup(latitude, longitude);
        const startDate = startdate.trim();
        const hours = "00";
        const minutes = "00";
        const date = moment.tz(`${startDate} ${hours}:${minutes}`, "MM/DD/YYYY H:m", timezone);
        return date.format();
    }
    return null;
}

/**
 * Given a raw wildfire event object of type B, returns a standardized representation of the object
 * that keeps all keys unused for climate gathering, but renames and/or removes keys used to create standardized keys
 * @param {Object} doc - The wildfire event object
 */
function processDocB(doc) {
    const picked = _.pick(doc, FEATURES_B);
    return {
        ..._.omit(doc, FEATURES_B),
        "Event": picked["incident_n"] ? picked["incident_n"].trim() : null,
        "Incident Name": picked["fire_name"] ? picked["fire_name"].trim() : null,
        "Start Date": processDateB(picked["start_date"], picked["start_hour"], picked["latitude"], picked["longitude"]),
        "Latitude": picked["latitude"] ? parseFloat(picked["latitude"]) : null,
        "Longitude": picked["longitude"] ? parseFloat(picked["longitude"]) : null,
        "State": picked["state"] ? picked["state"].trim() : null,
        "Size": picked["area_"] ? parseFloat(picked["area_"]) : null
    };
}

/**
 * Creates a time zone adjusted string representation of the given start date at the given latitude
 * and longitude for a raw wildfire event object of type B
 * @param {String} start_date - The string representation of a start date in MM/DD/YYYY format
 * @param {String} start_hour - The string representation of a time in HHmm 24 hour format
 * @param {Number} latitude - The floating point representation of a latitude coordinate
 * @param {Number} longitude - The floating point representation of a longitude coordinate
 */
function processDateB(start_date, start_hour, latitude, longitude) {
    if (start_date != null && start_hour != null && !isNaN(parseFloat(latitude)) && !isNaN(parseFloat(longitude))) {
        const timezone = tzlookup(latitude, longitude);
        const startDate = start_date.trim();
        const hours = start_hour.trim().substring(0, 2);
        const minutes = start_hour.trim().substring(2, 4);
        const date = moment.tz(`${startDate} ${hours}:${minutes}`, "MM/DD/YYYY H:m", timezone);
        return date.format();
    }
    return null;
}

/**
 * Given a raw wildfire event object of type C, returns a standardized representation of the object
 * that keeps all keys unused for climate gathering, but renames and/or removes keys used to create standardized keys
 * @param {Object} doc - The wildfire event object
 */
function processDocC(doc) {
    const picked = _.pick(doc, FEATURES_C);
    return {
        ..._.omit(doc, FEATURES_C),
        "Event": picked["uniquefireidentifier"] ? picked["uniquefireidentifier"].trim() : null,
        "Incident Name": picked["incidentname"] ? picked["incidentname"].trim() : null,
        "Start Date": processDateC(picked["firediscoverydatetime"], picked["latitude"], picked["longitude"]),
        "Latitude": picked["latitude"] ? parseFloat(picked["latitude"]) : null,
        "Longitude": picked["longitude"] ? parseFloat(picked["longitude"]) : null,
        "State": picked["state"] ? picked["state"].trim() : null,
        "Size": picked["acres"] ? parseFloat(picked["acres"]) : null
    };
}

/**
 * Creates a time zone adjusted string representation of the given datetime at the given latitude
 * and longitude for a raw wildfire event object of type C
 * @param {Number} datetime - The ms representation of a start date in Unix format
 * @param {Number} latitude - The floating point representation of a latitude coordinate
 * @param {Number} longitude - The floating point representation of a longitude coordinate
 */
function processDateC(datetime, latitude, longitude) {
    if (!isNaN(datetime) && !isNaN(latitude) && !isNaN(longitude)) {
        const timezone = tzlookup(latitude, longitude);
        const ms = parseInt(datetime);
        const date = moment.tz(ms, timezone);
        return date.format();
    }
    return null;
}

/**
 * Creates the arcgis.training collection by applying a transformation function 
 * to each wildfire event document in the arcgis.standardized collection.
 */
function createTrainingWildfires() {
    return new Promise((resolve, reject) => {
        const query = WILDFIRE_CONFIG.QUERY;
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = WILDFIRE_CONFIG.PRIMARY_DB_NAME;
        const outputCollectionName = WILDFIRE_CONFIG.PRIMARY_COLLECTION_NAME;
        createStandardizedView(query, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, WILDFIRE_CONFIG.TRANSFORM);
    });
}

/**
 * Creates the arcgis.map collection by restructuring the 
 * arcgis.training collection.
 */
function createTrainingWildfiresMap() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = WILDFIRE_CONFIG.PRIMARY_DB_NAME;
        const sourceCollectionName = WILDFIRE_CONFIG.PRIMARY_COLLECTION_NAME;
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
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
 * Creates the arcgis.format2 collection by restructuring the 
 * arcgis.training collection.
 */
function createTrainingWildfiresFormat2() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const sourceDbName = "arcgis";
        const sourceCollectionName = "training";
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "arcgis";
        const outputCollectionName = "format2";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            // update each doc, setting the event id as the primary id
            async.map(docs, (doc, mapCb) => {
                const eventId = doc["Event"];
                doc = _.omit(doc, "Event");
                doc["_id"] = eventId;
                mapCb(null, doc);
            }, (mapErr, mapRes) => {
                cb(null, mapRes);
            });
        });
    });
}



/**
 * Entry point for the wildfire data collection process.
 */
function collectWildfireData() {
    return new Promise((resolve, reject) => {
        downloadRaw()
            .then(createEventsFromRaw)
            .then(createUniqueFromEvents)
            .then(createWildfiresFromUnique)
            .then(createStandardizedWildfires)
            .then(createTrainingWildfires)
            .then(createTrainingWildfiresMap)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
}


/**
 * Uploads the arcgis.raw, arcgis.events, arcgis.unique, arcgis.wildfires, and arcgis.standardized 
 * collections to a backup database specified in the wildfire config object.
 */
function backupWildfireData() {
    const sourceDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
    const sourceDbName = WILDFIRE_CONFIG.PRIMARY_DB_NAME;
    const sourceCollectionNames = ["raw", "events", "unique", "wildfires", "standardized", "training", "format2"];
    const outputDbUrl = WILDFIRE_CONFIG.SECONDARY_MONGODB_URL;
    const outputDbName = WILDFIRE_CONFIG.SECONDARY_DB_NAME;
    const promisify = (sourceCollectionName) => {
        return new Promise((resolve, reject) => {
            const outputCollectionName = sourceCollectionName;
            loadSave({}, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }
    if (outputDbUrl && outputDbName) {
        // Upload each collection in parallel asynchronously
        return Promise.all(sourceCollectionNames.map(promisify));
    } else {
        return Promise.reject(new Error("Secondary database not specified, check config"));
    }
}

/**
 * Entry point for the wildfire data processing stage.
 */
function wildfireStages() {
    return new Promise((resolve, reject) => {
        collectWildfireData()
            .then(backupWildfireData)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
}

module.exports = {
    wildfireStages
}