// bucket schema
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

function getSchemaData(params) {
    const map = {
        bucket: 'buckets',
        account: 'account',
    };
    const level = Object.keys(params).filter(k => k in map)[0];
    const data = {};
    data.level = map[level];
    data.id = params[level];
    return data;
}

function getSchemaPrefix(params, timestamp) {
    const { level, id } = getSchemaData(params);
    const prefix = timestamp === undefined ? `s3:${level}:${id}:` :
        `s3:${level}:${timestamp}:${id}:`;
    return prefix;
}

/**
* Returns the metric key in schema for the bucket
* @param {string} params - bucket id
* @param {string} metric - metric id
* @param {number} timestamp - unix timestamp normalized to the nearest 15 min.
* @return {string} - schema key
*/

export function generateKey(params, metric, timestamp) {
    const prefix = getSchemaPrefix(params, timestamp);
    return keys[metric](prefix);
}

/**
* Returns a list of the counters for a bucket
* @param {string} params - bucket id
* @return {string[]} - array of keys for counters
*/
export function getCounters(params) {
    const prefix = getSchemaPrefix(params);
    return Object.keys(counters).map(
        item => counters[item](prefix));
}

/**
* Returns a list of all keys for a bucket
* @param {string} params - bucket id
* @param {number} timestamp - unix timestamp normalized to the nearest 15 min.
* @return {string[]} - list of keys
*/
export function getKeys(params, timestamp) {
    const prefix = getSchemaPrefix(params, timestamp);
    return Object.keys(keys)
        .map(item => keys[item](prefix));
}

/**
* Returns metric from key
* @param {string} key - schema key
* @param {string} value - the metric value
* @return {string} metric - S3 metric
*/
export function getMetricFromKey(key, value) {
    // s3:buckets:1473451689898:demo:putObject
    return key.slice(25).replace(`${value}:`, '');
}

/**
* Returns the keys representing state of the bucket
* @param {string} params - bucket id
* @return {string[]} - list of keys
*/
export function getStateKeys(params) {
    const prefix = getSchemaPrefix(params);
    return Object.keys(stateKeys)
        .map(item => stateKeys[item](prefix));
}

/**
* Returns the state metric key in schema for the bucket
* @param {string} params - bucket id
* @param {string} metric - metric id
* @return {string} - schema key
*/
export function generateStateKey(params, metric) {
    const prefix = getSchemaPrefix(params);
    return stateKeys[metric](prefix);
}

export function generateCounter(params, metric) {
    const prefix = getSchemaPrefix(params);
    return counters[metric](prefix);
}
