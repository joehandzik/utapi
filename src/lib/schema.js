// metric type schema
const stateKeys = {
    storageUtilized: prefix => `${prefix}storageUtilized`,
    numberOfObjects: prefix => `${prefix}numberOfObjects`,
};

const counters = {
    storageUtilizedCounter: prefix => `${prefix}storageUtilized:counter`,
    numberOfObjectsCounter: prefix => `${prefix}numberOfObjects:counter`,
};

const keys = {
    createBucket: prefix => `${prefix}CreateBucket`,
    deleteBucket: prefix => `${prefix}DeleteBucket`,
    listBucket: prefix => `${prefix}ListBucket`,
    getBucketAcl: prefix => `${prefix}GetBucketAcl`,
    putBucketAcl: prefix => `${prefix}PutBucketAcl`,
    putBucketWebsite: prefix => `${prefix}PutBucketWebsite`,
    deleteBucketWebsite: prefix => `${prefix}DeleteBucketWebsite`,
    getBucketWebsite: prefix => `${prefix}GetBucketWebsite`,
    listBucketMultipartUploads: prefix => `${prefix}ListBucketMultipartUploads`,
    listMultipartUploadParts: prefix => `${prefix}ListMultipartUploadParts`,
    initiateMultipartUpload: prefix => `${prefix}InitiateMultipartUpload`,
    completeMultipartUpload: prefix => `${prefix}CompleteMultipartUpload`,
    abortMultipartUpload: prefix => `${prefix}AbortMultipartUpload`,
    deleteObject: prefix => `${prefix}DeleteObject`,
    multiObjectDelete: prefix => `${prefix}MultiObjectDelete`,
    uploadPart: prefix => `${prefix}UploadPart`,
    getObject: prefix => `${prefix}GetObject`,
    getObjectAcl: prefix => `${prefix}GetObjectAcl`,
    putObject: prefix => `${prefix}PutObject`,
    copyObject: prefix => `${prefix}CopyObject`,
    putObjectAcl: prefix => `${prefix}PutObjectAcl`,
    headBucket: prefix => `${prefix}HeadBucket`,
    headObject: prefix => `${prefix}HeadObject`,
    incomingBytes: prefix => `${prefix}incomingBytes`,
    outgoingBytes: prefix => `${prefix}outgoingBytes`,
};

/**
* Creates the appropriate prefix for schema keys
* @param {object} params - object with metric type and id as a property
* @param {number} timestamp - (optional) unix timestamp normalized to the
* nearest 15 min.
* @return {string} - prefix for the schema key
*/
function getSchemaPrefix(params, timestamp) {
    // Map of possible metric types and their associated prefix level keys
    const map = {
        bucket: 'buckets',
        account: 'account',
        user: 'user',
    };
    const metricType = Object.keys(params).find(k => k in map);
    const level = map[metricType];
    const id = params[metricType];
    const prefix = timestamp === undefined ? `s3:${level}:${id}:` :
        `s3:${level}:${timestamp}:${id}:`;
    return prefix;
}

/**
* Returns the metric key for the metric type
* @param {object} params - object with metric type and id as a property
* @param {string} metric - metric to generate a key for
* @param {number} timestamp - unix timestamp normalized to the nearest 15 min.
* @return {string} - schema key
*/
export function generateKey(params, metric, timestamp) {
    const prefix = getSchemaPrefix(params, timestamp);
    return keys[metric](prefix);
}

/**
* Returns a list of the counters for a metric type
* @param {object} params - object with metric type and id as a property
* @return {string[]} - array of keys for counters
*/
export function getCounters(params) {
    const prefix = getSchemaPrefix(params);
    return Object.keys(counters).map(item => counters[item](prefix));
}

/**
* Returns a list of all keys for a metric type
* @param {object} params - object with metric type and id as a property
* @param {number} timestamp - unix timestamp normalized to the nearest 15 min.
* @return {string[]} - list of keys
*/
export function getKeys(params, timestamp) {
    const prefix = getSchemaPrefix(params, timestamp);
    return Object.keys(keys).map(item => keys[item](prefix));
}

/**
* Returns metric from key
* @param {string} key - schema key
* @param {string} value - the metric value
* @param {string} metricType - the metric type
* @return {string} metric - S3 metric
*/
export function getMetricFromKey(key, value, metricType) {
    // s3:user:1483048800000:foo-user:DeleteBucket
    if (metricType === 'user') {
        return key.slice(31).replace(`${value}:`, '');
    }
    // s3:buckets:1473451689898:demo:putObject
    // s3:account:1473451689898:demo:putObject
    return key.slice(25).replace(`${value}:`, '');
}

/**
* Returns the keys representing state of the metric type
* @param {object} params - object with metric type and id as a property
* @return {string[]} - list of keys
*/
export function getStateKeys(params) {
    const prefix = getSchemaPrefix(params);
    return Object.keys(stateKeys).map(item => stateKeys[item](prefix));
}

/**
* Returns the state metric key for the metric type
* @param {object} params - object with metric type and id as a property
* @param {string} metric - metric to generate a key for
* @return {string} - schema key
*/
export function generateStateKey(params, metric) {
    const prefix = getSchemaPrefix(params);
    return stateKeys[metric](prefix);
}

/**
* Returns the counter metric key for the metric type
* @param {object} params - object with metric type and id as a property
* @param {string} metric - metric to generate a key for
* @return {string} - schema key
*/
export function generateCounter(params, metric) {
    const prefix = getSchemaPrefix(params);
    return counters[metric](prefix);
}
