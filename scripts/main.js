const logger = require('./logger');

const pipeline = require('./pipeline');

function start() {
    pipeline
        .reduce((p, stage) => {
            return p.then(stage);
        }, Promise.resolve())
        .then(() => {
            logger.info('pipeline finished');
        })
        .catch(err => {
            logger.debug(err);
        });
}

start();