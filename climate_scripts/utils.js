const _ = require('lodash');
const { Transform } = require('stream');


module.exports.keyFuncStandard = function (obj) {
    return JSON.stringify(_.pick(obj, ["Event"]));
}

/**
 * Removes all duplicates from an array with a specified hash/uniqueness function
 * @param {Array} arr - The array to be filtered
 * @param {Function} key - The item uniqueness function
 */
module.exports.uniqueBy = function (arr, key) {
    const seen = {};
    return arr.filter(item => {
        const k = key(item);
        return seen.hasOwnProperty(k) ? false : (seen[k] = true);
    });
}

/**
 * Determines if a latitude and longitude coordinate are in the United States
 * https://gist.github.com/jsundram/1251783
 * @param {Number} latitude - The latitude to test 
 * @param {Number} longitude - The longitude to test
 */
module.exports.inBounds = function (latitude, longitude) {
    const TOP = 49.3457868; // north lat
    const LEFT = -124.7844079; // west long
    const RIGHT = -66.9513812 // east long
    const BOTTOM = 24.7433195 // south lat
    return BOTTOM <= latitude && latitude <= TOP && LEFT <= longitude && longitude <= RIGHT;
}

/**
 * Determines the sort order for a standardized wildfire event object based on date
 * @param {Object} doc1 - A standardized wildfire event object
 * @param {Object} doc2 - A standardized wildfire event object
 */
module.exports.dateComparator = function (doc1, doc2) {
    if (doc1["Start Date"] < doc2["Start Date"]) {
        return -1;
    } else if (doc1["Start Date"] > doc2["Start Date"]) {
        return 1;
    }
    return 0;
}

/**
 * Filters the given docs array, keeping only the objects with fewest null properties for each duplicate class
 * @param {Array<Object>} docs - The array of objects
 * @param {Function} keyFunc - The key function to represent unique objects
 */
module.exports.filterMaxInfo = function (docs, keyFunc) {

    // construct a hashmap containing array "buckets" as values for each class of duplicates
    const seen = {};
    for (let i = 0; i < docs.length; i++) {
        const doc = docs[i];
        const k = keyFunc(doc);
        if (seen.hasOwnProperty(k)) {
            // we've seen it before, push to its bucket
            seen[k].push(doc);
        } else {
            // create bucket
            seen[k] = [doc];
        }
    }

    // collect all max-info docs across each duplicate bucket
    return Object.keys(seen).reduce((arr, key) => {
        const duplicates = seen[key];
        return arr.concat(module.exports.maxInfo(duplicates));
    }, []);
}

/**
 * Returns the object with the max number of non-null properties from the given array of duplicates,
 * taking the first occurrence if there are multiple
 * @param {Array<Object>} duplicates - The array of objects
 */
module.exports.maxInfo = function (duplicates) {
    return _.omit(duplicates.map(d => {
        return {
            ...d,
            score: Object.keys(d).reduce((score, key) => {
                if (d[key] != null) {
                    return score + 1;
                }
                return score;
            }, 0)
        }
    }).reduce((acc, d) => {
        acc[0] = (acc[0] === undefined || d.score < acc[0].score) ? d : acc[0]
        acc[1] = (acc[1] === undefined || d.score > acc[1].score) ? d : acc[1]
        return acc;
    }, [])[1], "score");
}

// returns index of the first element in a such that f(element) > c
// returns -1 if such element does not exist
// a has to be sorted, f has to be an increasing function
module.exports.binarySearch = function (a, f, c) {
    if (f(a[0]) > c) return 0;
    if (f(a[a.length - 1]) <= c) return -1;
    var lo = 0;
    var hi = a.length - 1;
    while (lo < hi) {
        var mid = lo + (hi - lo) / 2;
        if (f(a[mid]) > c) hi = mid;
        else lo = mid + 1;
    }
    return hi;
}