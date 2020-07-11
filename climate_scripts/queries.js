const _ = require('lodash');

// 7,956 total "itype" events
const queryA = {
    // 7,536 total "WF" events
    isWildfire: { $and: [{ "itype": { $ne: null } }, { "itype": { $eq: "WF" } }] },
    hasStartDate: { "startdate": { $ne: null } },
    hasEventId: { $and: [{ "event_id": { $ne: null } }, { "event_id": { $ne: " " } }, { "event_id": { $ne: "" } }] },
    hasEventName: { $and: [{ "ename": { $ne: null } }, { "ename": { $ne: " " } }, { "ename": { $ne: "" } }] },
    hasLatLon: { $and: [{ "latdd": { $ne: null } }, { "longdd": { $ne: null } }] },
    inBounds: (top, left, bottom, right) => ({ $and: [{ "latdd": { $gte: bottom, $lte: top } }, { "longdd": { $gte: left, $lte: right } }] }),
    hasState: { $and: [{ "un_ustate": { $ne: null } }] },
    stateEquals: (state) => ({ "un_ustate": { $eq: state } }),
    hasAcres: { "acres": { $ne: null } },
    hasCosts: { "ecosts": { $ne: null } }
};

// 26,390 total "inc_type" events
const queryB = {
    // 25,667 total "WF" events
    isWildfire: { $and: [{ "inc_type": { $ne: null } }, { "inc_type": { $eq: "WF" } }] },
    hasStartDate: { $and: [{ "start_date": { $ne: null } }, { "start_hour": { $ne: null } }] },
    // TODO: Need to match documents with "start_date" as string with only a space i.e. " "
    hasEventId: { $and: [{ "incident_n": { $ne: null } }, { "incident_n": { $ne: " " } }, { "incident_n": { $ne: "" } }] },
    hasEventName: { $and: [{ "fire_name": { $ne: null } }, { "fire_name": { $ne: " " } }, { "fire_name": { $ne: "" } }] },
    hasLatLon: { $and: [{ "latitude": { $ne: null } }, { "longitude": { $ne: null } }] },
    inBounds: (top, left, bottom, right) => ({ $and: [{ "latitude": { $gte: bottom, $lte: top } }, { "longitude": { $gte: left, $lte: right } }] }),
    hasState: { $and: [{ "state": { $ne: null } }] },
    stateEquals: (state) => ({ "state": { $eq: state } }),
    hasAcres: { "area_": { $ne: null } }
};

// 5,480 total "incidenttypecategory" events
const queryC = {
    // 5,480 total "WF" events
    isWildfire: { $and: [{ "incidenttypecategory": { $ne: null } }, { "incidenttypecategory": { $eq: "WF" } }] },
    hasStartDate: { "firediscoverydatetime": { $ne: null } },
    hasEventId: { $and: [{ "uniquefireidentifier": { $ne: null } }, { "uniquefireidentifier": { $ne: " " } }, { "uniquefireidentifier": { $ne: "" } }] },
    hasEventName: { $and: [{ "incidentname": { $ne: null } }, { "incidentname": { $ne: " " } }, { "incidentname": { $ne: "" } }] },
    hasLatLon: { $and: [{ "latitude": { $ne: null } }, { "longitude": { $ne: null } }] },
    inBounds: (top, left, bottom, right) => ({ $and: [{ "latitude": { $gte: bottom, $lte: top } }, { "longitude": { $gte: left, $lte: right } }] }),
    hasState: { $and: [{ "state": { $ne: null } }] },
    stateEquals: (state) => ({ "state": { $eq: state } }),
    hasAcres: { "acres": { $ne: null } }
};

module.exports = {
    queryA,
    queryB,
    queryC
};