const logger = require('./logger');

const { inBounds } = require('./utils');

const WILDFIRE_CONFIG = {
    QUERY: { $and: [{ "State": { $eq: "CA" } }, { $or: [{ "Size": { $gte: 0 } }, { "Costs": { $gte: 0 } }] }] },
    PRIMARY_MONGODB_URL: process.env.PRIMARY_MONGODB_URL,
    PRIMARY_DB_NAME: 'arcgis',
    PRIMARY_COLLECTION_NAME: 'training',
    TRANSFORM: (docs) => {
        return docs.filter(doc => {
            const isInBounds = inBounds(doc["Latitude"], doc["Longitude"]);
            if (!isInBounds) {
                logger.warn(`event '${doc["Event"]}' not in bounds, (${doc["Latitude"]}, ${doc["Longitude"]})`);
            }
            return isInBounds;
        });
    },
    SECONDARY_MONGODB_URL: process.env.SECONDARY_MONGODB_URL,
    SECONDARY_DB_NAME: 'arcgis',
    SECONDARY_COLLECTION_NAME: 'training'
};

const CLIMATE_CONFIG = {
    apiKey: process.env.DARKSKY_API_KEY,
    interval: 'hourly',
    units: 336,
    limit: 5
};

const COMBINE_CONFIG = {
    units: CLIMATE_CONFIG.units,
    // props: ['temperature', 'windSpeed', 'humidity']
};

module.exports = {
    WILDFIRE_CONFIG,
    CLIMATE_CONFIG,
    COMBINE_CONFIG
}