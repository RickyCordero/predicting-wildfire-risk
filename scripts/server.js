const logger = require('./logger');

const express = require('express');
const app = express();

const PORT = 3000;

const { parse } = require('json2csv');

const { loadTransform } = require('./db');

app.get('/data', (req, res) => {
    const { events, features } = req.query;
    logger.info(JSON.stringify(events));
    logger.info(JSON.stringify(features));
    let query = {};
    if (events != "*") {
        query = {
            $or: events.split(',').reduce((acc, event) => {
                return acc.concat({ "Event": { $eq: event } });
            }, [])
        };
    }

    const projection = features.split(',').reduce((acc, f) => {
        // acc[`Features.${f}`] = 1;
        return acc;
    }, { "Event": 1 });

    ["Latitude", "Longitude", "Size", "Costs"].forEach((f) => {
        projection[f] = 1;
    });

    // const projection = {};

    const columns = features.split(',').reduce((acc, f) => {
        // return acc.concat({
        //     label: f,
        //     value: `Features.${f}`
        // });
        return acc;
    }, ["Event"]).concat(["Latitude", "Longitude", "Size", "Costs"]);

    logger.info(JSON.stringify(query));
    logger.info(JSON.stringify(projection));
    logger.info(JSON.stringify(columns));

    const sourceDbUrl = "mongodb://localhost:27017";
    const sourceDbName = "training";
    const sourceCollectionName = "training2";

    loadTransform(query, projection, sourceDbUrl, sourceDbName, sourceCollectionName, (queryError, queryResult) => {
        if (queryError) {
            logger.debug(JSON.stringify(queryError));
            res.send(queryError);
        } else {
            logger.info('sending query results');
            // res.send(parse(queryResult));
            csv = parse(queryResult, {
                // fields: columns
            });
            // csv = parse(queryResult);
            // console.log(csv);
            res.send(csv);
            // res.send(queryResult);
        }
    });
});

app.listen(PORT, () => {
    console.log(`app listening on port ${PORT}`);
});