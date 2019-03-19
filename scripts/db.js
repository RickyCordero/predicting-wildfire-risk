const logger = require('./logger');

const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
const streamify = require('stream-array');

const { Transform } = require('stream');

const utils = require('./utils');

/**
 * Saves data to a mongo collection
 * @param {MongoDb} db - The mongo database object
 * @param {Array<Object>} data - The array of objects to be saved
 * @param {String} outputCollectionName - The name of the output collection
 * @param {Function} cb - The next function to be called
 */
module.exports.saveToDBBuffer = (db, data, outputCollectionName, cb) => {
    const collection = db.collection(outputCollectionName);
    collection.insertMany(data, (err, res) => {
        if (err) {
            cb(err);
        } else {
            cb(null, res);
        }
    });
};

/**
 * Creates a transform stream that streams a wildfire event object array to the output database
 * specified in the configuration object
 * @param {Object} config - The configuration object containing the following:
 *  * @param {String} dbName - The name of the output mongo database
 *  * @param {String} dbURL - The output mongo url
 *  * @param {String} dbConnection - The mongo database connection
 *  * @param {Number} batchSize - The max number of documents to write at once
 *  * @param {String} collection - The name of the output mongo collection
 */
module.exports.saveToDB = (config) => new Transform({
    writableObjectMode: true, // accept an array
    readableObjectMode: true, // pass an array
    transform(chunk, _encoding, callback) {
        // where the data will end up
        const writableStream = streamToMongoDB(_.pick(config, ['dbURL', 'dbConnection', 'batchSize', 'collection']));
        // create a read stream from the given chunk array
        const readStream = streamify(chunk);
        // consume the writable stream
        readStream
            .pipe(writableStream)
            .on('error', (err) => {
                logger.warn(`yo, there was an error writing to ${config.dbName}/${config.collection}`);
                logger.debug(err);
            })
            .on('finish', () => {
                logger.info(`saved ${chunk.length} item(s) to ${config.dbName}/${config.collection}`);
                this.push(chunk); // pass the chunk to the next stream
                callback();
            });
    }
});

/**
 * Creates a transform stream that streams a wildfire event object to the output database
 * specified in the configuration object
 * @param {Object} config - The configuration object containing the following:
 *  * @param {String} dbName - The name of the output mongo database
 *  * @param {String} dbURL - The output mongo url
 *  * @param {String} dbConnection - The mongo database connection
 *  * @param {Number} batchSize - The max number of documents to write at once
 *  * @param {String} collection - The name of the output mongo collection
 */
module.exports.saveToDBEach = (config) => new Transform({
    writableObjectMode: true, // accept an object
    readableObjectMode: true, // pass an object
    transform(chunk, _encoding, callback) {

        // where the data will end up
        const writableStream = streamToMongoDB(_.pick(config, ['dbURL', 'dbConnection', 'batchSize', 'collection']));

        // create a read stream from the given chunk object
        const readStream = streamify([chunk]);

        // consume the writable stream
        readStream
            .pipe(writableStream)
            .on('error', (err) => {
                logger.warn(`yo, there was an error writing to ${config.dbName}/${config.collection}`);
                logger.debug(err);
            })
            .on('finish', () => {
                logger.info(`saved chunk to ${config.dbName}/${config.collection}`);
                this.push(chunk); // pass the chunk to the next stream
                callback();
            });
    }
});

/**
 * Loads a collection using a mongo connection object
 * @param {MongoClient} dbClient - The mongo connection object
 * @param {String} dbName - The name of the database for which data should be retrieved
 * @param {String} collectionName - The name of the collection for which data should be retrieved
 * @param {Function} cb - The next function to be called
 */
module.exports.loadFromDBBuffer = (dbClient, dbName, collectionName, cb) => {
    const db = dbClient.db(dbName);
    const collection = db.collection(collectionName);
    collection.find({}).toArray((err, items) => {
        if (err) {
            cb(err);
        } else {
            cb(null, items);
        }
    });
}

/**
 * Combines multiple collections from multiple databases into a single output collection in a database,
 * and applies the transform function, if given, to each element before saving
 * @param {Array<Object>} inputs - The array of input objects containing source database and collection name key-value pairs containing the following properties:
 *  * @param {Object} query - The mongo query object to be used for filtering 
 *  * @param {String} sourceDbName - The name of the source database
 *  * @param {String} sourceCollectionName - The name of the source database
 *  * @param {String} sourceDbUrl - The mongo url of the source database
 *  * @param {Function} sourceTransform - The function to be applied to each element in the particular collection
 * @param {String} outputDbName - The name of the output database
 * @param {String} outputCollectionName - The name of the output collection
 * @param {Function} cb - The next function to call
 * @param {Array<Function>} transforms - The array of functions to recursively be applied to the resultant combined collection
 */
module.exports.combineMany = (inputs, outputDbUrl, outputDbName, outputCollectionName, cb, ...transforms) => {
    async.map(inputs, (item, mapCb) => {
        const { query, sourceDbName, sourceCollectionName, sourceDbUrl, sourceTransform } = item;
        MongoClient.connect(sourceDbUrl, (dbError, dbClient) => {
            if (dbError) {
                mapCb(dbError);
            } else {
                const sourceDb = dbClient.db(sourceDbName);
                const sourceCollection = sourceDb.collection(sourceCollectionName);
                sourceCollection.find(query ? query : {}).toArray((err, docs) => {
                    if (err) {
                        mapCb(err);
                    } else {
                        const transformedDocs = sourceTransform ? docs.map(sourceTransform) : docs;
                        mapCb(null, transformedDocs);
                    }
                    dbClient.close();
                });
            }
        });
    }, (mapError, mapResult) => {
        if (mapError) {
            cb(mapError);
        } else {
            const combined = mapResult.reduce((acc, arr) => {
                return acc.concat(arr);
            }, []);
            const final = utils.applyAll(transforms, combined);
            MongoClient.connect(outputDbUrl, (dbError, dbClient) => {
                if (dbError) {
                    cb(dbError);
                } else {
                    const outputDb = dbClient.db(outputDbName);
                    module.exports.saveToDBBuffer(outputDb, final, outputCollectionName, (err, _res) => {
                        if (err) {
                            cb(err);
                        } else {
                            cb(null, `saved combined data to ${outputDbName}/${outputCollectionName}`);
                        }
                    });
                }
                dbClient.close();
            });
        }
    });
}

/**
 * Loads a collection from a database, processes all documents in the sub-collection generated by the given query
 * object using the transform function, saves the resultant transformed documents to an output collection and database
 * @param {Object} query - The mongo query object to be used for filtering
 * @param {String} sourceDbUrl - The mongo url of the source database
 * @param {String} sourceDbName - The source database name
 * @param {String} sourceCollectionName - The source collection name
 * @param {String} outputDbUrl - The mongo url of the destination database
 * @param {String} outputDbName - The destination database name
 * @param {String} outputCollectionName - The destination collection name
 * @param {Function} callback - The next function to be called
 * @param {Function} transform - The function to apply to the array of documents from the source collection
 */
module.exports.loadSave = (query, sourceDbUrl, sourceDbName, sourceCollectionName, outputDbUrl, outputDbName, outputCollectionName, callback, transform) => {
    MongoClient.connect(sourceDbUrl, (sourceDbError, sourceDbClient) => {
        if (sourceDbError) {
            logger.warn('yo, there was an error connecting to the local database');
            callback(sourceDbError);
        } else {
            MongoClient.connect(outputDbUrl, (outputDbError, outputDbClient) => {
                if (outputDbError) {
                    logger.warn('yo, there was an error connecting to the local database');
                    callback(outputDbError);
                } else {
                    const sourceDb = sourceDbClient.db(sourceDbName);
                    const sourceCollection = sourceDb.collection(sourceCollectionName);
                    sourceCollection.find(query).toArray((sourceQueryError, sourceQueryResults) => {
                        if (sourceQueryError) {
                            logger.warn('yo, there was an error querying the source collection');
                            callback(sourceQueryError);
                        } else {
                            const outputDb = outputDbClient.db(outputDbName);
                            if (transform) {
                                transform(sourceQueryResults, (err, transformedDocs) => {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        logger.info('attempting to save to db now');
                                        module.exports.saveToDBBuffer(outputDb, transformedDocs, outputCollectionName, (saveError, _saveResult) => {
                                            if (saveError) {
                                                logger.warn(`yo, there was an error saving to ${outputDbName}/${outputCollectionName}`);
                                                callback(saveError);
                                            } else {
                                                logger.info(`saved data to ${outputDbName}/${outputCollectionName}`);
                                                callback();
                                            }
                                            outputDbClient.close();
                                            sourceDbClient.close();
                                        });
                                    }
                                });
                            } else {
                                module.exports.saveToDBBuffer(outputDb, sourceQueryResults, outputCollectionName, (saveError, _saveResult) => {
                                    if (saveError) {
                                        logger.warn(`yo, there was an error saving to ${outputDbName}/${outputCollectionName}`);
                                        callback(saveError);
                                    } else {
                                        logger.info(`saved data to ${outputDbName}/${outputCollectionName}`);
                                        callback();
                                    }
                                    outputDbClient.close();
                                    sourceDbClient.close();
                                });
                            }
                        }
                    });
                }
            });
        }
    });
}