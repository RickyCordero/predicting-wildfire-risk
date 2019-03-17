const logger = require('./logger');

/*
    Sample dark sky historical weather response object at the daily interval (with time transformation):

            https://darksky.net/dev/docs#data-point-object
            {
                "time": "2007-07-05T07:00:00.000Z",
                "summary": "Mostly cloudy throughout the day.",
                "icon": "partly-cloudy-day",
                "sunriseTime": 1183640022,
                "sunsetTime": 1183691908,
                "moonPhase": 0.68,
                "precipIntensity": 0,
                "precipIntensityMax": 0,
                "precipProbability": 0,
                "temperatureHigh": 85.11,
                "temperatureHighTime": 1183669200,
                "temperatureLow": 50.99,
                "temperatureLowTime": 1183716000,
                "apparentTemperatureHigh": 85.11,
                "apparentTemperatureHighTime": 1183669200,
                "apparentTemperatureLow": 50.99,
                "apparentTemperatureLowTime": 1183716000,
                "dewPoint": 51.24,
                "humidity": 0.64,
                "pressure": 1011.77,
                "windSpeed": 1.91,
                "windGust": 13.12,
                "windGustTime": 1183690800,
                "windBearing": 260,
                "cloudCover": 0.63,
                "uvIndex": 10,
                "uvIndexTime": 1183665600,
                "visibility": 6.51,
                "temperatureMin": 55.28,
                "temperatureMinTime": 1183701600,
                "temperatureMax": 85.11,
                "temperatureMaxTime": 1183669200,
                "apparentTemperatureMin": 55.28,
                "apparentTemperatureMinTime": 1183701600,
                "apparentTemperatureMax": 85.11,
                "apparentTemperatureMaxTime": 1183669200
            }
*/

/**
 * Constructs an array of objects corresponding to training data
 * @param {Object} config - The configuration object containing the following properties:
 *  * @param {String} interval - A string representing the level of time granularity to include in each request object
 *  * @param {Number} units - The number of intervals of data to collect after each wildfire's ignition time
 *  * @param {Array<String>} props - The array of climate data properties corresponding to descriptive features
 *  * @param {Array<Object>} wildfires - The array of wildfire event objects
 *  * @param {Array<Object>} climateData - The array of climate data objects where each object corresponds to a single day of hourly or daily data
 * @returns {Array<Object>} - The array of objects corresponding to the final training dataset
 */
const combineData = (config) => {
    /*
     goal:
        {
            event: wildfire['Incident Name'],
            t_n: climate['temperature'],
            ...
            tn: climate['temperature'],
            w_n: climate['windSpeed'],
            ...
            wn: climate['windSpeed'],
            h_n: climate['humidity'],
            ...
            hn: climate['humidity'],
            area: wildfire['Size']
        }
    */
    // no field means the intervally array exists but is empty
    // null value means no intervally data was found from the api
    return config.wildfires.reduce((res, wildfire) => {
        const climateDatum = config.climateData.find(d => d["Event"] == wildfire["Event"]);
        let events = [];
        if (climateDatum) { // if we find a climate datum with same id as wildfire
            if (climateDatum[config.interval]) { // if climate datum has data for certain interval
                const features = climateDatum[config.interval]
                    .map((datum, idx) => {
                        if (datum.error) { // if datum had an error
                            logger.warn(`climate datum with id '${climateDatum.id}' had an error, returning null for each prop`);
                            logger.warn(datum.error);
                        }
                        const label = idx - config.units < 0 ?
                            `_${config.units - idx}` : idx - config.units;
                        return config.props.reduce((acc, prop) => {
                            return ({ ...acc, [`${prop}${label}`]: datum[prop] })
                        }, {});
                    })
                    .reduce((acc, obj) => ({ ...acc, ...obj }), {});
                events.push({
                    'id': wildfire['id'],
                    'event': wildfire['Incident Name'],
                    ...features,
                    'area': wildfire['Size']
                });

            } else {
                logger.warn(`climate datum with id '${climateDatum.id}' does not contain interval '${config.interval}', not using`);
            }
        }
        return res.concat(events);
    }, []);
};

// TODO: Implement this function using loadSave
function cleanClimateData() {
    // if (config.interval == 'hourly') {
    //     // remove all extra data points collected in overshot data requests
    //     climateResult[config.interval] = climateResult[config.interval].filter(datum => {
    //         return moment.parseZone(datum.time).isBetween(startDate, endDate, 'hour', '[]');
    //     });
    // }
    if (obj[interval] && obj[interval].data && obj[interval].data.length > 0) {
        if (interval == 'hourly' && obj[interval].data.length == 24 || interval == 'daily' && obj[interval].data.length == 1) {
            const offset = startDate.utcOffset();
            obj[interval].data.forEach(x => {
                // populate a particular interval's array of data points in the results object
                /**
                 * The UNIX time at which this data point begins. minutely data point are always aligned to the top
                 *  of the minute, hourly data point objects to the top of the hour, and daily data point objects to
                 *  midnight of the day, all according to the local time zone.
                 */
                results[interval].push(Object.assign(x, { 'time': moment.unix(x.time).utcOffset(offset).format() }));
            });
        } else {
            const sizeError = `not enough data points found for '${interval}' data, found only ${obj[interval].data.length} point(s) for (${latitude},${longitude})`;
            logger.warn(sizeError);
            const sizeErrorObj = { time: time, error: sizeError };
            results[interval].push(sizeErrorObj);
        }
    } else {
        const intervalError = `no '${interval}' data found for (${latitude},${longitude})`;
        logger.warn(intervalError);
        const intervalErrorObj = { time: time, error: intervalError };
        results[interval].push(intervalErrorObj);
    }
}

module.exports = {
    combineData
}