const logger = require('./logger');

const async = require('async');
const MongoClient = require('mongodb').MongoClient;
const { filterFires, getFileNames, getFileNamesStream, loadHtml, convertHtmlToJsonObjects, cleanWildfireData, indexWildfires, arrayLength } = require('./wildfire');
const DATA_FOLDER = process.env.DATA_FOLDER;

const WILDFIRE_DB_NAME = "wildfires";
const WILDFIRE_COLLECTION_NAME = `default`;
const CLIMATE_DB_NAME = "climate";
const CLIMATE_COLLECTION_NAME = "default";
const TRAINING_DB_NAME = "training";
const TRAINING_COLLECTION_NAME = "default";

const WILDFIRE_CONFIG = {
    folder: DATA_FOLDER,
    // years: ['2004'],
    // locations: ['AK'], // norcal, socal
    props: ['Incident Name', 'Start Date', 'Latitude', 'Longitude', 'Size', 'Costs']
};

/**
 * Entry point for the wildfire data collection and transformation process.
 * @param {Object} wildfireConfig - A configuration object containing properties for collecting climate data
 * @param {Object} climateConfig - A configuration object containing properties for filtering wildfire event objects 
 * @param {Object} wildfireOutputDbConfig - A configuration object for saving to the wildfire database
 * @param {Object} climateOutputDbConfig - A configuration object for saving to the climate database
 * @param {Object} dbClient - A mongodb client object
 */
const start = () => {
    const s = process.hrtime();
    MongoClient.connect("mongodb://localhost:27017", (dbError, dbClient) => {
        if (dbError) {
            logger.warn('yo, there was an error connecting to the database');
            logger.debug(dbError);
        } else {
            const WILDFIRE_OUTPUT_DB_CONFIG = {
                dbURL: "",
                dbConnection: dbClient.db(WILDFIRE_DB_NAME),
                batchSize: 50,
                collection: WILDFIRE_COLLECTION_NAME
            };
            const CLIMATE_OUTPUT_DB_CONFIG = {
                dbURL: "",
                dbConnection: dbClient.db(CLIMATE_DB_NAME),
                batchSize: 50,
                collection: CLIMATE_COLLECTION_NAME
            };
            logger.info('successfully connected to database client');
            logger.info('WILDFIRE_CONFIG = ' + JSON.stringify(WILDFIRE_CONFIG, null, 4));
            const fileNames = getFileNames(WILDFIRE_CONFIG);
            logger.info(`found ${fileNames.length} file(s) matching config parameters`);
            async.map(fileNames, (fileName, mapCallback) => {
                const leaf = path.parse(fileName).base;
                loadHtml(fileName)
                    // string to array
                    .pipe(convertHtmlToJsonObjects())
                    // array to array
                    .pipe(cleanWildfireData(WILDFIRE_CONFIG))
                    // array to array
                    .on('data', data => {
                        logger.info(`processing chunk of length ${data.length} for file ${leaf}`);
                        logger.info(JSON.stringify(data, null, 4));
                    })
                    .pipe(saveToDB(WILDFIRE_OUTPUT_DB_CONFIG))
                    // .pipe(getClimateData(CLIMATE_CONFIG))
                    // .pipe(saveToDB({ ...CLIMATE_OUTPUT_DB_CONFIG, fileName: leaf }))
                    .pipe(filterFires)
                    .on('finish', () => {
                        logger.info(`done processing file ${leaf}`);
                        mapCallback();
                    });
            }, (mapError, mapResult) => {
                logger.info('done processing all files');
                dbClient.close();
                logger.info(process.hrtime(s).join('.') + " seconds elapsed");
            });
        }
    });
};