// This is the main script for scraping all of the ics-209 data tables available at
// https://fam.nwcg.gov/fam-web/hist_209/report_list_209

const dataGetter = require('./ics-209-report-getter').ics209ReportGetter;
const path = require('path');

/**
 * Path to output folder. Ensures we get consistent folder placement no matter what pwd is.
 * @type {void | Promise<void> | Promise<string>}
 */
const outputFolder = path.resolve(__dirname + '/../output');

/**
 * All of the locations that are available on the server.
 * @type {*[]}
 */
const allLocations = [
{
	locationCode: 'AK',
	locationName: 'alaska',
},
{
	locationCode: 'EA',
	locationName: 'eastern-area',
},
{
	locationCode: 'EB',
	locationName: 'eastern-great-basin',
},
{
	locationCode: 'FC',
	locationName: 'national-interagency-fire-center',
},
{
	locationCode: 'NO',
	locationName: 'northern-california',
},
{
	locationCode: 'NR',
	locationName: 'northern-rockies',
},
{
	locationCode: 'NW',
	locationName: 'northwest',
},
{
	locationCode: 'RM',
	locationName: 'rocky-mountains'
},
{
	locationCode: 'SA',
	locationName: 'southern-area'
},
{
	locationCode: 'SO',
	locationName: 'southern-california'
},
{
	locationCode: 'SW',
	locationName: 'southwest'
},
{
	locationCode: 'WB',
	locationName: 'western-great-basin'
}
];

/**
 * Main method. Pulls all of the ics-209 reports for the locations for the inputted years.
 * @param outputFolder		The folder to save the data in
 * @param locations			Array of objects containing the locationCode and locationName
 * @param startYear			First year to pull data from (inclusive)
 * @param endYear			Pull data up to, but not including this year
 */
const main = (outputFolder, locations, startYear, endYear) => {
	for (let year=startYear; year<endYear; year++) {
		for (let j=0; j<locations.length; j++) {
			const currentLocation = locations[j]
			dataGetter(year, currentLocation.locationCode, currentLocation.locationName, outputFolder
				+ `/${year}`);
		}
	}
};

// Call the main mehtod to collect the data in [2003, 2014). Note that 2002 does not respect the address formatting of
// the other years on the server, so we cannot auto-pull it here
main(outputFolder, allLocations, 2003, 2014);