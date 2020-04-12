const logger = require('./logger');

const fs = require('fs');
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
 * Creates the arcgis.nonfire collection by loading the nonfire JSON
 */
function createNonFireEvents() {
    return new Promise((resolve, reject) => {
        const outputDbUrl = WILDFIRE_CONFIG.PRIMARY_MONGODB_URL;
        const outputDbName = "arcgis";
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


module.exports = {
    createNonFireEvents
}