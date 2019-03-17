const arrayChunkExample = () => {
    /**
     * Reading from one mongodb collection using array chunked streams
     */
    const MongoClient = require('mongodb').MongoClient;
    const { testProperty } = require('./utils');
    const { Readable } = require('stream');
    const streamify = require('stream-array');


    // const query = {};
    const query = { $and: [{ "Latitude": { $gt: 30 } }, { "Latitude": { $lt: 32.5 } }] };
    const sourceDbUrl = "mongodb://localhost:27017/";
    const sourceDbName = "arcgis";
    const sourceCollectionName = "training";
    const outputDbUrl = "mongodb://localhost:27017/";
    const outputDbName = "test";
    const outputCollectionName = "test";

    MongoClient.connect(sourceDbUrl, (sourceDbError, sourceDbClient) => {
        if (sourceDbError) {
            console.log(sourceDbError);
        } else {
            const sourceDb = sourceDbClient.db(sourceDbName);
            const sourceCollection = sourceDb.collection(sourceCollectionName);

            sourceCollection.find(query).toArray((queryError, queryResults) => {
                if (queryError) {
                    console.log(queryError);
                } else {
                    // const readStream = new Readable({
                    //     // writableObjectMode: true, // read an array
                    //     // readableObjectMode: true, // write an array
                    //     read() {
                    //         this.push(new Buffer(queryResults));
                    //         this.push(null);
                    //     }
                    // });
                    const item = { "a": "hello", "b": "waddup" };
                    const readStream = new Readable({
                        writableObjectMode: true,
                        readableObjectMode: true, // write an array
                        read() { this.push(item); this.push(null); }
                    });
                    // const readStream = streamify([queryResults]);
                    const writableStream = testProperty();

                    // consume the writable stream
                    readStream
                        .on('data', (chunk) => {
                            console.log(JSON.stringify(chunk, null, 4));
                            // console.log([...chunk]);
                        })
                        // .pipe(writableStream)
                        .on('end', () => {
                            console.log('done!');
                            sourceDbClient.close();
                        });
                }
            });

        }
    });
};

const objectChunkExample = () => {
    /**
     * Reading from one mongodb collection using object chunked streams
     */
    const MongoClient = require('mongodb').MongoClient;
    const streamToMongoDB = require('stream-to-mongo-db').streamToMongoDB;
    const { testProperty } = require('./utils');
    const streamify = require('stream-array');
    const query = { $and: [{ "Latitude": { $gt: 30 } }, { "Latitude": { $lt: 32.5 } }] };
    const sourceDbUrl = "mongodb://localhost:27017/";
    const sourceDbName = "arcgis";
    const sourceCollectionName = "training";
    const outputDbUrl = "mongodb://localhost:27017/";
    const outputDbName = "test";
    const outputCollectionName = "test";

    MongoClient.connect(sourceDbUrl, (sourceDbError, sourceDbClient) => {
        if (sourceDbError) {
            console.log(sourceDbError);
        } else {
            const sourceDb = sourceDbClient.db(sourceDbName);
            const sourceCollection = sourceDb.collection(sourceCollectionName);

            const readStream = sourceCollection.find(query).stream();
            const writableStream = testProperty();

            // both of the below methods pass an object in each array as the chunk object 

            const method1 = () => {
                // consume the writable stream
                readStream
                    .pipe(writableStream)
                    .on('finish', () => {
                        console.log('done!');
                        sourceDbClient.close();
                    });
            };

            const method2 = () => {
                // working example of streaming each object
                sourceCollection.find(query).toArray((queryError, queryResults) => {
                    if (queryError) {
                        console.log(queryError);
                    } else {

                        // Each element in the array will be pushed into the piped stream, without modifying the source array.
                        const readStream = streamify(queryResults);
                        const writableStream = testProperty();

                        // consume the writable stream
                        readStream
                            .pipe(writableStream)
                            .on('finish', () => {
                                console.log('done!');
                                sourceDbClient.close();
                            });
                    }
                });
            };

            // method1();

        }
    });
};

/**
 * Example 1.5: Stream from another MongoDB database (transforming the input data before writing it to the writable stream)
 */
const example1_5 = () => {
    // where the data will come from
    const inputDBConfig = { dbUrl: 'mongodb://localhost:27017/yourInputDBHere', collection: 'yourCollectionHere' };

    // where the data will end up
    const outputDBConfig = { dbUrl: 'mongodb://localhost:27017/streamToMongoDB', collection: 'devTestOutput' };

    MongoClient.connect(inputDBConfig.dbUrl, (err, db) => {
        if (err) throw err;

        // create the writable stream
        const writableStream = streamToMongoDB(outputDBConfig);

        // create the readable stream and transform the data before writing it
        const stream = db.collection(inputDBConfig.collection).find().stream({
            transform: (doc) => {
                // do whatever you like to the doc
                doc.whoIsAwesome = 'StreamToMongoDBIsAwesome';
            }
        });

        stream.pipe(writableStream);

        stream.on('end', () => {
            console.log('done!');
            db.close();
        });
    });
};

/**
 * Example 3: Stream from a Web API
 */
const example3 = () => {
    const JSONStream = require('JSONStream');

    // where the data will end up
    const outputDBConfig = { dbURL: 'mongodb://localhost:27017/streamToMongoDB', collection: 'devTestOutput' };

    // create the writable stream
    const writableStream = streamToMongoDB(outputDBConfig);

    // create readable stream and consume it
    request('www.pathToYourApi.com/endPoint')
        .pipe(JSONStream.parse('*'))
        .pipe(writableStream);
};

/**
 * Example 4: Stream from a local file
 */
const example4 = () => {
    const JSONStream = require('JSONStream');
    const fs = require('fs');

    // where the data will end up
    const outputDBConfig = { dbURL: 'mongodb://localhost:27017/streamToMongoDB', collection: 'devTestOutput' };

    // create the writable stream
    const writableStream = streamToMongoDB(outputDBConfig);

    // create readable stream and consume it
    fs.createReadStream('./myJsonData.json')
        .pipe(JSONStream.parse('*'))
        .pipe(writableStream);
};

arrayChunkExample();