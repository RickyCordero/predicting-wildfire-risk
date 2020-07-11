const logger = require('./logger');

const fs = require('fs');
const path = require('path');
const htmlTableToJson = require('html-table-to-json');
const _ = require('lodash');
const tzlookup = require('tz-lookup');
const moment = require('moment-timezone');

/* 
    11 total properties for a fire in date range 2002 - 2006 inclusively:

    ["Incident Number", "Incident Name", "Start Date", "IC Name", "Team Type", "Latitude", "Longitude", "Size", "Costs", "Controlled Date", "Structures Destroyed"]


    14 total properties for a fire in date range 2007 - 2013 inclusively:

    ["State-Unit", "Incident Number", "Incident Name", "Incident Type", "Start Date", "IC Name", "Team Type", "Latitude", "Longitude", "Size", "Measurement", "Costs", "Controlled Date", "Structures Destroyed"]

*/

/**
 * Entry point for obtaining historical wildfire data. Collects and returns an array of wildfire event objects that satisfy
 * the event filters specified in the given config object.
 * @param {Object} config - A configuration object containing properties for filtering wildfire event objects
 * @returns {Array<Object>} - An array containing wildfire event objects
 */
const getWildfireData = (config) => {
    const dirPath = path.join(__dirname, config.folder);
    const fileNames =
        // get array of file names found in the data folder
        fs.readdirSync(dirPath)
            // concatenate arrays of all file names found in each resultant year folder into a single array
            .reduce((resultArray, yearFolder) =>
                resultArray.concat(fs.readdirSync(dirPath + yearFolder).map(fileName => dirPath + yearFolder + '\\' + fileName)), [])
            // choose only the file names corresponding to a location specified in the config object's location array
            .filter(fileName => config.locations != null ?
                config.locations.reduce((bool, location) => bool || fileName.includes(location), false) : true)
            // choose only the file names corresponding to a year specified in the config object's years array
            .filter(fileName => config.years != null ?
                config.years.reduce((bool, year) => bool || fileName.includes(year), false) : true)
            // ignore national-interagency-fire-center-FC-incident-summary files since they are empty
            .filter(fileName => !fileName.includes('FC'));


    const wildfires = fileNames.reduce((wildfireData, fileName) =>
        wildfireData.concat(getData(fileName, { ...config, fileName })), []);

    return wildfires;
};

/**
 * Reads an html file and returns its string representation
 * @param {String} filePath - The file path to the html file
 */
const loadHtml = (filePath) => {
    return fs.readFileSync(filePath, 'utf8');
};

/**
 * Constructs a list of objects where each object corresponds to a unique wildfire event with properties given by the specified config object
 * @param {String} filePath - A string representing an html file path containing a wildfire data table
 * @param {Object} config - A configuration object containing properties used for filtering wildfire event object properties
 * @returns {Array<Object>} - A filtered array of objects where each element represents a unique wildfire event with properties given by the specified config object
 */
function getData(filePath, config) {
    const htmlString = loadHtml(filePath);
    const rows = convertHtmlToJsonObjects(htmlString);
    return transformRows(rows, config);
}

/**
 * Converts an html string to an array of json objects
 * @param {String} html - An html string containing an html table
 * @returns {Array<Object>} - An array of objects
 */
function convertHtmlToJsonObjects(html) {
    const jsonTables = new htmlTableToJson(html);
    return jsonTables.results[0];
}

/**
 * Converts a date string to a UTC timezone sensitive date string
 * @param {String} dateString - The string representation of the date given by the wildfire data
 * @param {Number} latitude - The latitude corresponding to a wildfire event location
 * @param {Number} longitude - The longitude corresponding to a wildfire event location
 * @returns {String} - The string representation of the given date with a timezone generated  UTC offset
 */
const convertToDate = (dateString, latitude, longitude) => {
    const timezone = tzlookup(latitude, longitude);
    const [d, t] = dateString.trim().split(' ');
    const hours = t.substring(0, 2);
    const minutes = t.substring(2, 4);
    const date = moment.tz(`${d} ${hours}:${minutes}`, "MM/DD/YYYY H:m", timezone);
    return date.format();
};

/**
 * Converts a string representation of a comma separated integer to an integer
 * Example: 1,000,000 -> 1000000
 * @param {String} s - The string representing the integer
 */
const stringToInteger = s => {
    // remove all commas then convert to integer
    return parseInt(s.replace(/,/g, ""));
};

/**
 * Updates all data types of each object in the given array
 * @param {Array<Object>} objects - An array of event objects that need their properties updated
 */
const updateDataTypes = objects => {
    return objects.map(obj => {

        // names with commas need quotes for safe csv export
        if (obj["Incident Name"] != null && obj["Incident Name"].trim() != '') {
            if (obj["Incident Name"].includes(',')) {
                obj["Incident Name"] = `"${obj["Incident Name"].trim()}"`;
            }
        }

        // longitude values negated for accurate coordinates

        if (obj["Start Date"] != null && obj["Start Date"].trim() != '') {
            obj["Start Date"] = convertToDate(obj["Start Date"].trim(), parseFloat(obj["Latitude"].trim()), -parseFloat(obj["Longitude"].trim()));
        }
        if (obj["Controlled Date"] != null && obj["Controlled Date"].trim() != '') {
            obj["Controlled Date"] = convertToDate(obj["Controlled Date"].trim(), parseFloat(obj["Latitude"].trim()), -parseFloat(obj["Longitude"].trim()));
        }
        if (obj["Latitude"] != null && obj["Latitude"].trim() != '') {
            obj["Latitude"] = parseFloat(obj["Latitude"].trim());
        }
        if (obj["Longitude"] != null && obj["Longitude"].trim() != '') {
            obj["Longitude"] = -parseFloat(obj["Longitude"].trim());
        }
        if (obj["Size"] != null && obj["Size"].trim() != '') {
            obj["Size"] = stringToInteger(obj["Size"].trim());
        }
        if (obj["Costs"] != null && obj["Costs"].trim() != '') {
            obj["Costs"] = stringToInteger(obj["Costs"].trim().replace("$", ""));
        }
        if (obj["Structures Destroyed"] != null && obj["Structures Destroyed"].trim() != '') {
            obj["Structures Destroyed"] = stringToInteger(obj["Structures Destroyed"].trim());
        }
        // don't include measurement, if all events are wildfires
        return _.omit(obj, ['Measurement']);
    });
};

/**
 * Removes all objects from the rows array that do not pertain to a new
 *  fire, or are missing features we care about as specified by the indexes array
 * @param {Array<Object>} rows - The array of objects to filter
 * @param {Array<Integer>} indexes - The array of indexes to be used for filtering
 * @param {Object} keyMap - The object representing the original mapping from indexes to property names
 * @param {Object} config - The configuration object
 */
const getValidWildfires = (rows, indexes, keyMap, config) => {

    const rowHasAllProps = row => indexes.reduce((bool, index) =>
        bool && (row[index] != null), true);

    const rowHasAllNonEmptyProps = row => indexes.reduce((bool, index) =>
        bool && (row[index].trim() != ''), true);

    // by default, include only rows with all properties specified by the indexes array
    let conditions = [rowHasAllProps];

    /**
     * when custom properties passed, for each prop, make sure row has a respective column entry.
     * otherwise, no filtering will be done on prop emptiness so keyMap's properties will be used 
     * for this collection of rows, meaning all properties need not be empty
     */
    if (config.props) {
        conditions.push(rowHasAllNonEmptyProps);
    }

    // remove all rows with measurements indicating non wildfire events
    if (Object.values(keyMap).includes('Measurement')) {
        const measurementKey = Object.keys(keyMap).find(key => keyMap[key] == 'Measurement');
        // don't include rows containing SQ MILES as a measurement
        const rowIsAWildfire = row => row[measurementKey] != null && row[measurementKey].trim() != 'SQ MILES';
        conditions.push(rowIsAWildfire);
    }

    // remove all rows with incident names indicating non wildfire events
    if (Object.values(keyMap).includes('Incident Name')) {
        const incidentNameKey = Object.keys(keyMap).find(key => keyMap[key] == 'Incident Name');
        // don't include earthquakes
        const rowIsNotEarthquake = row => !row[incidentNameKey].trim().toUpperCase().includes('EARTHQUAKE');
        // don't include hurricanes
        const rowIsNotHurricane = row => !row[incidentNameKey].trim().toUpperCase().includes('HURRICANE');
        // don't include typhoons
        const rowIsNotTyphoon = row => !row[incidentNameKey].trim().toUpperCase().includes('TYPHOON');
        // don't include controlled burns
        const rowIsNotControlledBurn = row => !row[incidentNameKey].trim().toUpperCase().includes('WFU');

        conditions.push(row => row[incidentNameKey] != null &&
            [rowIsNotEarthquake, rowIsNotHurricane, rowIsNotTyphoon, rowIsNotControlledBurn].reduce((bool, c) => bool && c(row), true));
    }

    // keep all rows with incident type indicating a wildfire event from 2007 - 2013
    if (Object.values(keyMap).includes('Incident Type')) {
        const incidentTypeKey = Object.keys(keyMap).find(key => keyMap[key] == 'Incident Type');
        const rowIsAWildfire = row => row[incidentTypeKey] != null && row[incidentTypeKey].trim() == 'WF';
        conditions.push(rowIsAWildfire);
    }

    // keep all rows with valid coordinates
    if (Object.values(keyMap).includes('Longitude') && Object.values(keyMap).includes('Latitude')) {
        if (config.props) { // if latitude and longitude asked for in config
            const latitudeKey = Object.keys(keyMap).find(key => keyMap[key] == 'Latitude');
            const longitudeKey = Object.keys(keyMap).find(key => keyMap[key] == 'Longitude');
            const rowHasValidGeocoordinates = row =>
                row[longitudeKey] != null && row[longitudeKey].trim() != '0' &&
                row[latitudeKey] != null && row[latitudeKey].trim() != '0';
            conditions.push(rowHasValidGeocoordinates);
        }
    }

    // keep only the rows that satisfy all conditions
    return rows.filter(row => conditions.reduce((bool, c) => bool && c(row), true));

};

/**
 * Updates the properties of each object in the wildfires array
 * @param {Array<Object>} wildfires - The array of wildfire event objects
 * @param {Array<String>} filteredProps - The array of strings containing all properties to use for filtering
 * @param {Object} keyMap - The object representing the mapping from indexes to property names
 * @returns {Array<Object>} - The array of updated wildfire objects
 */
const updateProperties = (wildfires, filteredProps, keyMap) => {
    if (Object.values(keyMap).includes('Measurement')) { // 2007 - 2013
        // use key map to change key names for each object, 
        // then pick subset of keys (specified by filteredProps) from each object
        return wildfires.map(row =>
            _.pick(_.mapKeys(row, (_value, key) => keyMap[key]), filteredProps));
    } else { // 2002 - 2006
        // extract the size and measurement unit, then replace object with a new object containing those properties
        return wildfires.map(row => {
            let obj = _.pick(_.mapKeys(row, (_value, key) => keyMap[key]), filteredProps);
            if (filteredProps.includes('Size') || filteredProps.includes('Measurement')) {
                if (obj["Size"] != null) {
                    const cellContents = obj["Size"].trim().split(" ");
                    let size = cellContents[0]; // get the number or unit (if number not given)
                    let measurement = '';
                    if (isNaN(size.replace(/,/g, ""))) { // check if size with no commas is a number
                        measurement = size;
                        size = '0';
                    } else {
                        measurement = cellContents.slice(1).join(' '); // concatenate all following text
                    }
                    // don't include measurement by default
                    obj = filteredProps.includes('Measurement') ? { ...obj, "Size": size, "Measurement": measurement } : { ...obj, "Size": size };
                } else {
                    obj = filteredProps.includes('Measurement') ? { ...obj, "Size": '0', "Measurement": 'ACRES' } : { ...obj, "Size": '0' };
                }
            }
            return obj;
        });
    }
};

/**
 * Filters and picks the necessary data out of each object in the given array of objects
 * @param {Array<Object>} rows - An array of objects corresponding to each row in the converted html table
 * @param {Array<String>} config - A configuration object containing properties for filtering and constructing wildfire event objects
 * @returns {Array<Object>} - An array of objects corresponding to the set of wildfire events matching criteria in the given configuration object
 */
const transformRows = (rows, config) => {
    // pop first element, this corresponds to the row of column names
    const keyMap = rows.shift();
    let filteredMap = keyMap;
    let filteredProps = Object.values(filteredMap);
    if (config.props != null) {
        // all properties we actually want to use
        filteredMap = Object.keys(keyMap).reduce((obj, key) => {
            if (config.props.includes(keyMap[key])) {
                return { ...obj, [key]: keyMap[key] };
            } else {
                return obj;
            }
        }, {});
        filteredProps = Object.values(filteredMap);
        // if config.props does not contain 'Measurement' and does not contain 'Size', don't add 'Measurement'
        // if config.props contains 'Measurement' (and necessarily 'Size'), then filteredProps contains 'Measurement', so don't add it
        // if config.props does not contain 'Measurement' but contains 'Size', then filteredProps does not contain 'Measurement', so add it
        if (!filteredProps.includes('Measurement') && config.props.includes("Size")) {
            filteredProps.push('Measurement');
        }
    } else {
        // if keyMap contains 'Measurement' (2007-2013) then filteredProps contains 'Measurement', so don't add it
        // if keyMap does not contain 'Measurement' (2002-2006) then filteredProps does not contain 'Measurement', so add it
        if (!filteredProps.includes('Measurement')) {
            filteredProps.push('Measurement');
        }
    }

    // all indexes in the key map that map exactly to one prop for each prop in props
    const indexes = Object.keys(filteredMap);

    // remove all degenerate rows from the converted json array
    const validWildfires = getValidWildfires(rows, indexes, keyMap, config);

    // update property names for each wildfire object
    const updatedPropertiesWildfires = updateProperties(validWildfires, filteredProps, keyMap);

    // update data types for each wildfire object
    const updatedDataTypesWildfires = updateDataTypes(updatedPropertiesWildfires);

    return updatedDataTypesWildfires;
};

module.exports = {
    loadHtml,
    getData,
    convertHtmlToJsonObjects,
    convertToDate,
    stringToInteger,
    updateDataTypes,
    getValidWildfires,
    updateProperties,
    transformRows
}