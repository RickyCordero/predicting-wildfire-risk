const { climateStages } = require('./darksky');
const { combineStages } = require('./combine');
const { wildfireStages } = require('./arcgis');

module.exports = [combineStages];