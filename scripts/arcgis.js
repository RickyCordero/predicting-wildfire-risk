const logger = require('./logger');

const async = require('async');
const request = require('request');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
const tzlookup = require('tz-lookup');
const moment = require('moment-timezone');

const { saveToDBBuffer, loadSave } = require('./db');
const { dateComparator, filterMaxInfo } = require('./utils');
const { queryA, queryB, queryC } = require('./queries');

const FEATURES_A = ["startdate", "ename", "latdd", "longdd", "un_ustate", "acres", "ecosts", "event_id"];
const FEATURES_B = ["start_date", "start_hour", "fire_name", "latitude", "longitude", "state", "area_", "area_meas", "incident_n"];
const FEATURES_C = ["firediscoverydatetime", "incidentname", "latitude", "longitude", "state", "acres", "uniquefireidentifier"];

/**
 * Downloads raw wildfire data from the ArcGIS REST API from 2002 to 2018
 * and saves it to the local arcgis.raw collection
 * https://rmgsc-haws1.cr.usgs.gov/arcgis/sdk/rest/index.html#//02ss0000006v000000
 * id=26 => 2002
 * id=25 => 2003
 * ...
 * id=10 => 2018
 */
function downloadRaw() {
    return new Promise((resolve, reject) => {
        MongoClient.connect("mongodb://localhost:27017", (dbError, dbClient) => {
            if (dbError) {
                logger.warn('yo, there was an error connecting to the local database');
                reject(dbError);
            } else {
                const yearIds = [];
                for (let i = 10; i <= 26; i++) {
                    yearIds.push(i);
                }
                async.map(yearIds, (yearId, mapCb) => {
                    const url = `https://wildfire.cr.usgs.gov/ArcGIS/rest/services/geomac_dyn/MapServer/${yearId}/query?where=1%3D1&outFields=*&outSR=4326&f=json`;
                    request(url, (err, _res, body) => {
                        if (err) {
                            mapCb(err);
                        } else {
                            const obj = JSON.parse(body);
                            mapCb(null, obj);
                        }
                    });
                }, (mapErr, mapRes) => {
                    if (mapErr) {
                        logger.warn('yo, there was a map error');
                        reject(mapErr);
                    } else {
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
 * Creates the local arcgis.events collection from the local arcgis.raw collection
 */
function createEventsFromRaw() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "arcgis";
        const sourceCollectionName = "raw";
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "arcgis";
        const outputCollectionName = "events";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            cb(null, docs.reduce((acc, obj) => {
                const rows = obj.features.map(f => f.attributes);
                return acc.concat(rows);
            }, []));
        });
    });
}

/**
 * Creates the local arcgis.unique collection from the local arcgis.events collection
 */
function createUniqueFromEvents() {
    return new Promise((resolve, reject) => {
        const query = {};
        const sourceDbUrl = "mongodb://localhost:27017";
        const sourceDbName = "arcgis";
        const sourceCollectionName = "events";
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "arcgis";
        const outputCollectionName = "unique";
        loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs, cb) => {
            cb(null, filterMaxInfo(docs, (doc) => {
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
 * Creates a view of unique events
 * @param {Object} query - The mongo query object
 * @param {String} outputDbUrl - The mongo url of the output database
 * @param {String} outputDbName - The name of the output database
 * @param {String} outputCollectionName - The name of the output collection
 * @param {Function} callback - The next function to call
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createUniqueView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "arcgis";
    const sourceCollectionName = "unique";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Creates the local arcgis.wildfires collection from the local arcgis.unique collection
 */
function createWildfiresFromUnique() {
    return new Promise((resolve, reject) => {
        const query = {
            $or: [
                queryA.isWildfire,
                queryB.isWildfire,
                queryC.isWildfire
            ]
        };
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "arcgis";
        const outputCollectionName = "wildfires";
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
 * Creates a view of unique wildfire events
 * @param {Object} query - The mongo query object to be used for filtering
 * @param {String} outputDbUrl - The mongo url of the destination database
 * @param {String} outputDbName - The destination database name
 * @param {String} outputCollectionName - The destination collection name
 * @param {Function} callback - The next function to be called
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createWildfireView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "arcgis";
    const sourceCollectionName = "wildfires";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Creates the local arcgis.standardized collection from the local arcgis.wildfires collection
 * where each document represents a wildfire event, each document has uniform key names for a base 
 * set of keys defined in each processDoc function
 */
function createStandardizedWildfires() {
    return new Promise((resolve, reject) => {
        const query = {};
        const outputDbUrl = "mongodb://localhost:27017";
        const outputDbName = "arcgis";
        const outputCollectionName = "standardized";
        createWildfireView(query, outputDbUrl, outputDbName, outputCollectionName, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        }, (docs) => {
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
 * Creates a view of standardized unique wildfire events
 * @param {Object} query - The mongo query object to be used for filtering
 * @param {String} outputDbUrl - The mongo url of the destination database
 * @param {String} outputDbName - The destination database name
 * @param {String} outputCollectionName - The destination collection name
 * @param {Function} callback - The next function to be called
 * @param {Function} transform - The function to apply to the array of documents from the source collection query results
 */
function createStandardizedView(query, outputDbUrl, outputDbName, outputCollectionName, callback, transform) {
    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "arcgis";
    const sourceCollectionName = "standardized";
    loadSave(query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform);
}

/**
 * Given a raw wildfire event object of type A, returns a standardized representation of the object
 * that keeps all keys unused for climate gathering, but renames and/or removes keys used to create standardized keys
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
 * Uploads the arcgis.raw, arcgis.events, arcgis.unique, arcgis.wildfires, and arcgis.standardized collections to the remote database
 */
function upload() {
    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "arcgis";
    const sourceCollectionNames = ["raw", "events", "unique", "wildfires", "standardized"];
    const outputDbUrl = process.env.MONGODB_URL;
    const outputDbName = "arcgis";
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
    return Promise.all(sourceCollectionNames.map(promisify));
}

/**
 * Entry point for the wildfire data collection stage.
 */
function collectWildfireData() {
    return new Promise((resolve, reject) => {
        downloadRaw()
            .then(createEventsFromRaw)
            .then(createUniqueFromEvents)
            .then(createWildfiresFromUnique)
            .then(createStandardizedWildfires)
            .then(resolve)
            .catch(err => {
                reject(err);
            });
    });
}

module.exports = {
    collectWildfireData,
    createUniqueView,
    createWildfireView,
    createStandardizedView
}