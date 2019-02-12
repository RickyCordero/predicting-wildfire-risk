/**
GOAL:

For each year of data:
	For each location in the year:
		1. Get the data
		2. Extract the html table element
		3. Save it to disk in a folder for the year, with a name corresponding to the year and location
*/

const request = require('request');
const fs = require('fs');
const shelljs = require('shelljs');
const jsdom = require('jsdom');
const { JSDOM } = jsdom;

/**
 * Function for getting a single ics-209 report, and saving the raw html table from it.
 * @param year                      The year for the report to get
 * @param locationCode              The location code of the report to get
 * @param locationName              A human-readable representation of the location the report is for
 * @param parentFolderForSaving     The folder that the generated file should be saved in
 */
const ics209ReportGetter = (year, locationCode, locationName, parentFolderForSaving) => {
    const frontOfURL = 'https://fam.nwcg.gov/fam-web/hist_209/hist_';
    const endOfURLPreParams = '_r_209_gacc_sprd';
    const keyForLocation = 'v_gaid';

    // Set up the link we will make our request to
    const fullLink = frontOfURL + year + endOfURLPreParams;

    request({
            method: 'Get',
            uri: fullLink,
            qs: {
                [keyForLocation]: locationCode
            }
        },
        (error, response, body) => {
            if (error) {
                console.log(`Something went wrong fetching data from ${fullLink} for year ${year} and locationCode ${locationCode}`);
                throw error;
            } else {
                console.log(`Successfully fetched data from ${fullLink} for year ${year} and locationCode ${locationCode}`);
                handleData(body, year, locationCode, locationName, parentFolderForSaving);
            }

        });
};

/**
 * Ingests the body of the response received from the server, extracts the html table containing the data, and saves
 * it in parentFolder as an html file with name: '<year>-<locationName>-<locationCode>-incident-summary.html'.
 * @param body             The body of the response received from the ICS-209 report server
 * @param year             The year for the report
 * @param locationCode     The location code for the report
 * @param locationName     A human readable representation of the location the report is for
 * @param parentFolder     The folder to save the generated file in
 */
const handleData = (body, year, locationCode, locationName, parentFolder) => {
    const dom = new JSDOM(body);

    // Get the table
    const theTable = dom.window.document.querySelector('table:first-of-type');

    // Make sure its not undefined
    if (theTable.outerHTML) {

        // Save the table to the proper file 
        const filePath = `${parentFolder}/${year}-${locationName}-${locationCode}-incident-summary.html`;

        // fs.mkdir(parentFolder, {recursive: true}, (error) => {
       	// 	// Don't throw error if folder already existed
        // 	if (error && error.code !== 'EEXIST') {
        // 		throw error;
        // 	}
        // });

        shelljs.mkdir('-p', parentFolder);

        fs.writeFile(filePath, theTable.outerHTML, (error) => {
            if (error) {
                throw error;
            } else {
                console.log(`Successfully saved data to ${filePath}`);
            }
        });
    } else {
        throw new error('Unable to get raw html for the table');
    }

};

module.exports.ics209ReportGetter = ics209ReportGetter;


