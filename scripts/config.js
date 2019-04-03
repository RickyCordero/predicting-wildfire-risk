const logger = require('./logger');

const { inBounds } = require('./utils');

const WILDFIRE_CONFIG = {
    query: { $and: [{ "State": { $eq: "CA" } }, { $or: [{ "Size": { $gte: 0 } }, { "Costs": { $gte: 0 } }] }] },
    outputDbUrl: 'mongodb://localhost:27017',
    outputDbName: 'arcgis',
    outputCollectionName: 'training',
    transform: (docs) => {
        return docs.filter(doc => {
            const isInBounds = inBounds(doc["Latitude"], doc["Longitude"]);
            if (!isInBounds) {
                logger.warn(`event '${doc["Event"]}' not in bounds, (${doc["Latitude"]}, ${doc["Longitude"]})`);
            }
            return isInBounds;
        });
    }
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