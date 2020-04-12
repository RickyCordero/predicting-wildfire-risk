const { wildfireStages } = require('./arcgis');
const { climateStages } = require('./darksky');
const { combineStages } = require('./combine');

module.exports = [wildfireStages, climateStages, combineStages];