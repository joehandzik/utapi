import assert from 'assert';
import { Logger } from 'werelogs';
import Datastore from './Datastore';
import { generateKey, generateCounter, getCounters, generateStateKey }
    from './schema';
import { errors } from 'arsenal';
import redisClient from '../utils/redisClient';

const methods = {
    createBucket: '_pushMetricCreateBucket',
    deleteBucket: '_genericPushMetric',
    listBucket: '_genericPushMetric',
    getBucketAcl: '_genericPushMetric',
    putBucketAcl: '_genericPushMetric',
    putBucketWebsite: '_genericPushMetric',
    getBucketWebsite: '_genericPushMetric',
    deleteBucketWebsite: '_genericPushMetric',
    uploadPart: '_pushMetricUploadPart',
    initiateMultipartUpload: '_pushMetricInitiateMultipartUpload',
    completeMultipartUpload: '_pushMetricCompleteMultipartUpload',
    listMultipartUploads: '_pushMetricListBucketMultipartUploads',
    listMultipartUploadParts: '_genericPushMetric',
    abortMultipartUpload: '_genericPushMetric',
    deleteObject: '_genericPushMetricDeleteObject',
    multiObjectDelete: '_genericPushMetricDeleteObject',
    getObject: '_pushMetricGetObject',
    getObjectAcl: '_genericPushMetric',
    putObject: '_pushMetricPutObject',
    copyObject: '_pushMetricCopyObject',
    putObjectAcl: '_genericPushMetric',
    headBucket: '_genericPushMetric',
    headObject: '_genericPushMetric',
};

const metricTypes = ['bucket', 'account'];

export default class UtapiClient {
    constructor(config) {
        this.disableClient = true;
        this.log = null;
        this.ds = null;
        // setup logger
        if (config && config.log) {
            this.log = new Logger('UtapiClient', { level: config.log.level,
                    dump: config.log.dumpLevel });
        } else {
            this.log = new Logger('UtapiClient', { level: 'info',
                dump: 'error' });
        }
        // setup datastore
        if (config && config.redis) {
            this.ds = new Datastore()
                .setClient(redisClient(config.redis, this.log));
            this.disableClient = false;
        }
    }

    /**
    * Normalizes timestamp precision to the nearest 15 minutes to
    * reduce the number of entries in a sorted set
    * @return {number} timestamp - normalized to the nearest 15 minutes
    */
    static getNormalizedTimestamp() {
        const d = new Date();
        const minutes = d.getMinutes();
        return d.setMinutes((minutes - minutes % 15), 0, 0);
    }

    /**
    * set datastore
    * @param {DataStore} ds - Datastore instance
    * @return {object} current instance
    */
    setDataStore(ds) {
        this.ds = ds;
        this.disableClient = false;
        return this;
    }

    /*
    * Utility function to use when callback is not defined
    */
    _noop() {}

   /**
    * Check the types of optional `params` object properties. This enforces
    * object properties for particular push metric calls and checks the
    * existence of at least one metric type (e.g., bucket or account).
    * @param {object} params - params object with metric data
    * @param {string} params.bucket - (optional) bucket name
    * @param {string} params.account - (optional) account ID
    * @param {number} params.byteLength - (optional) size of an object deleted
    * @param {number} params.newByteLength - (optional) new object size
    * @param {number|null} params.oldByteLength - (optional) old object size
    * (for object overwrites). This value can be `null` for a new object,
    * or >= 0 for an existing object with content-length 0 or greater than 0.
    * @param {number} params.numberOfObjects - (optional) number of obects
    * @param {array} properties - (option) properties to assert types for
    * @return {undefined}
    */
    _checkTypes(params, properties = []) {
        const types = metricTypes.filter(type => type in params);
        assert(types.length, 'Metric object must include a metric type');
        types.forEach(metric => assert(typeof params[metric] === 'string',
            `${metric} property must be a string`));
        properties.forEach(prop => {
            assert(params[prop] !== undefined, 'Metric object must include ' +
                `${prop} property`);
            if (prop === 'oldByteLength') {
                assert(typeof params[prop] === 'number' ||
                    params[prop] === null, 'oldByteLength  property ' +
                    'must be an integer or `null`');
            } else {
                assert(typeof params[prop] === 'number', `${prop} ` +
                    'property must be an integer');
            }
        });
    }

    /*
    * Utility function to use with push metric calls.
    */
    _generateKeyAndIncrement(params, timestamp, action, log, callback) {
        const key = generateKey(params, action, timestamp);
        return this.ds.incr(key, err => {
            if (err) {
                log.error('error incrementing counter', {
                    method: `UtapiClient.${methods[action]}`,
                    error: err,
                });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }

    _getLogObject(params, method, timestamp) {
        const obj = {
            method: `UtapiClient.${method}`,
            timestamp,
        };
        const type = metricTypes.filter(type => type in params);
        assert(type.length === 1, 'Cannot log more than one metric type');
        obj[type] = params[type];
        return obj;
    }

     /*
     * Utility function to log the metric being pushed.
     */
    _logMetric(params, method, timestamp, log) {
        const logObject = this._getLogObject(params, method, timestamp);
        log.trace('pushing metric', logObject);
    }

     /*
     * Creates an array of parameter objects for each metric type.
     */
    _getParamsArr(params) {
        const arr = [];
        const types = metricTypes.filter(type => type in params);
        types.forEach(type => {
            const typeObj = {};
            typeObj[type] = params[type];
            Object.keys(params).forEach(k => {
                if (types.indexOf(k) < 0) {
                    typeObj[k] = params[k];
                }
            });
            arr.push(typeObj);
        });
        return arr;
    }

    /**
    * Generic method exposed by the client to push a metric with some values.
    * `params` can be expanded to provide metrics for other granularities
    * (e.g. service, account, user).
    * @param {string} metric - metric to be published
    * @param {string} reqUid - Request Unique Identifier
    * @param {object} params - params object with metric data
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account id
    * @param {number} params.byteLength - (optional) size of an object deleted
    * @param {number} params.newByteLength - (optional) new object size
    * @param {number|null} params.oldByteLength - (optional) old object size
    * (for object overwrites). This value can be `null` for a new object,
    * or >= 0 for an existing object with content-length 0 or greater than 0.
    * @param {number} params.numberOfObjects - (optional) number of obects
    * added/deleted
    * @param {callback} [cb] - (optional) callback to call
    * @return {object} this - current instance
    */
    pushMetric(metric, reqUid, params, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        const log = this.log.newRequestLoggerFromSerializedUids(reqUid);
        const timestamp = UtapiClient.getNormalizedTimestamp();
        const paramsArray = this._getParamsArr(params);
        // Push metrics for each metric type included in the `params` object.
        paramsArray.forEach(metricParams => {
            if (this[methods[metric]] === this._genericPushMetric) {
                this._genericPushMetric(metricParams, timestamp, metric, log,
                    callback);
            } else {
                this[methods[metric]](metricParams, timestamp, log, callback);
            }
        });
        return this;
    }

    /**
    * Updates counter for CreateBucket action on a Bucket resource. Since create
    * bucket occcurs only once in a bucket's lifetime, counter is always 1
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricCreateBucket(params, timestamp, log, callback) {
        this._checkTypes(params);
        this._logMetric(params, '_pushMetricCreateBucket', timestamp, log);
        // set storage utilized and number of objects counters to 0,
        // indicating the start of the bucket timeline
        const cmds = getCounters(params).map(item => ['set', item, 0]);
        cmds.push(
            // remove old timestamp entries
            ['zremrangebyscore',
                generateStateKey(params, 'storageUtilized'), timestamp,
                timestamp],
            ['zremrangebyscore', generateStateKey(params, 'numberOfObjects'),
                timestamp, timestamp],
            // add new timestamp entries
            ['set', generateKey(params, 'createBucket', timestamp), 1],
            ['zadd', generateStateKey(params, 'storageUtilized'), timestamp,
                0],
            ['zadd', generateStateKey(params, 'numberOfObjects'), timestamp, 0]
        );
        return this.ds.batch(cmds, err => {
            if (err) {
                log.error('error incrementing counter', {
                    method: 'Buckets.pushMetricCreateBucket',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }

    /**
    * Updates counter for an action
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} timestamp - normalized timestamp of current time
    * @param {string} action - action metric to update
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _genericPushMetric(params, timestamp, action, log, callback) {
        this._checkTypes(params);
        this._logMetric(params, '_genericPushMetric', timestamp, log);
        return this._generateKeyAndIncrement(params, timestamp, action, log,
            callback);
    }

    /**
    * Updates counter for UploadPart action on an object in a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} params.newByteLength - size of object in bytes
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricUploadPart(params, timestamp, log, callback) {
        this._checkTypes(params, ['newByteLength']);
        const { newByteLength } = params;
        this._logMetric(params, '_pushMetricUploadPart', timestamp, log);
        // update counters
        return this.ds.batch([
            ['incrby', generateCounter(params, 'storageUtilizedCounter'),
                newByteLength],
            ['incrby', generateKey(params, 'incomingBytes', timestamp),
                newByteLength],
            ['incr', generateKey(params, 'uploadPart', timestamp)],
        ], (err, results) => {
            if (err) {
                log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricUploadPart',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            // storage utilized counter
            const actionErr = results[0][0];
            const actionCounter = results[0][1];
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricUploadPart',
                    metric: 'storage utilized',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }

            return this.ds.batch([
                ['zremrangebyscore',
                    generateStateKey(params, 'storageUtilized'),
                    timestamp, timestamp],
                ['zadd', generateStateKey(params, 'storageUtilized'),
                    timestamp, actionCounter],
            ], callback);
        });
    }

    /**
    * Updates counter for Initiate Multipart Upload action on a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricInitiateMultipartUpload(params, timestamp, log, callback) {
        this._checkTypes(params);
        this._logMetric(params, '_pushMetricInitiateMultipartUpload', timestamp,
            log);
        return this._generateKeyAndIncrement(params, timestamp,
            'initiateMultipartUpload', log, callback);
    }

    /**
    * Updates counter for Complete Multipart Upload action on a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricCompleteMultipartUpload(params, timestamp, log, callback) {
        this._checkTypes(params);
        this._logMetric(params, '_pushMetricCompleteMultipartUpload', timestamp,
            log);
        return this.ds.batch([
            ['incr', generateCounter(params, 'numberOfObjectsCounter')],
            ['incr', generateKey(params, 'completeMultipartUpload',
                timestamp)],
        ], (err, results) => {
            if (err) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricCompleteMultipartUpload',
                    metric: 'number of objects',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            // number of objects counter
            const actionErr = results[0][0];
            const actionCounter = results[0][1];
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricCompleteMultipartUpload',
                    metric: 'number of objects',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            const key = generateStateKey(params, 'numberOfObjects');
            return this.ds.batch([
                ['zremrangebyscore', key, timestamp, timestamp],
                ['zadd', key, timestamp, actionCounter],
            ], callback);
        });
    }

    /**
    * Updates counter for ListMultipartUploads action on a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricListBucketMultipartUploads(params, timestamp, log, callback) {
        this._checkTypes(params);
        this._logMetric(params, '_pushMetricListBucketMultipartUploads',
            timestamp, log);
        const key = generateKey(params, 'listBucketMultipartUploads',
            timestamp);
        return this.ds.incr(key, err => {
            if (err) {
                log.error('error incrementing counter', {
                    method: 'UtapiClient.pushMetricListBucketMultipart' +
                        'Uploads',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }

    /**
    * Updates counter for DeleteObject or MultiObjectDelete action
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} params.byteLength - size of the object deleted
    * @param {number} params.numberOfObjects - number of objects deleted
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _genericPushMetricDeleteObject(params, timestamp, log, callback) {
        this._checkTypes(params, ['byteLength', 'numberOfObjects']);
        const { byteLength, numberOfObjects } = params;
        const bucketAction = numberOfObjects === 1 ? 'deleteObject'
            : 'multiObjectDelete';
        this._logMetric(params, '_genericPushMetricDeleteObject', timestamp,
            log);
        return this.ds.batch([
            ['decrby', generateCounter(params, 'storageUtilizedCounter'),
                byteLength],
            ['decrby', generateCounter(params, 'numberOfObjectsCounter'),
                numberOfObjects],
            ['incr', generateKey(params, bucketAction, timestamp)],
        ], (err, results) => {
            if (err) {
                log.error('error incrementing counter', {
                    method: 'UtapiClient._genericPushMetricDeleteObject',
                    error: err,
                });
                return callback(errors.InternalError);
            }

            const cmds = [];
            // storage utilized counter
            let actionErr = results[0][0];
            let actionCounter = parseInt(results[0][1], 10);
            actionCounter = actionCounter < 0 ? 0 : actionCounter;
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient._genericPushMetricDeleteObject',
                    metric: 'storage utilized',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            cmds.push(
                ['zremrangebyscore',
                    generateStateKey(params, 'storageUtilized'),
                    timestamp, timestamp],
                ['zadd',
                    generateStateKey(params, 'storageUtilized'),
                    timestamp, actionCounter]);

            // num of objects counter
            actionErr = results[1][0];
            actionCounter = parseInt(results[1][1], 10);
            actionCounter = actionCounter < 0 ? 0 : actionCounter;
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient._genericPushMetricDeleteObject',
                    metric: 'num of objects',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            cmds.push(
                ['zremrangebyscore',
                    generateStateKey(params, 'numberOfObjects'), timestamp,
                    timestamp],
                ['zadd', generateStateKey(params, 'numberOfObjects'),
                    timestamp, actionCounter]);
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for GetObject action on an object in a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} params.newByteLength - size of object in bytes
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricGetObject(params, timestamp, log, callback) {
        this._checkTypes(params, ['newByteLength']);
        const { newByteLength } = params;
        this._logMetric(params, '_pushMetricGetObject', timestamp, log);
        // update counters
        return this.ds.batch([
            ['incrby', generateKey(params, 'outgoingBytes', timestamp),
                newByteLength],
            ['incr', generateKey(params, 'getObject', timestamp)],
        ], err => {
            if (err) {
                log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricGetObject',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }


    /**
    * Updates counter for PutObject action on an object in a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} params.newByteLength - size of object in bytes
    * @param {number} params.oldByteLength - previous size of object
    * in bytes if this action overwrote an existing object
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricPutObject(params, timestamp, log, callback) {
        this._checkTypes(params, ['newByteLength', 'oldByteLength']);
        const { newByteLength, oldByteLength } = params;
        let numberOfObjectsCounter;
        // if previous object size is null then it's a new object in a bucket
        // or else it's an old object being overwritten
        if (oldByteLength === null) {
            numberOfObjectsCounter = ['incr', generateCounter(params,
                'numberOfObjectsCounter')];
        } else {
            numberOfObjectsCounter = ['get', generateCounter(params,
                'numberOfObjectsCounter')];
        }
        const oldObjSize = oldByteLength === null ? 0 : oldByteLength;
        const storageUtilizedDelta = newByteLength - oldObjSize;
        this._logMetric(params, '_pushMetricPutObject', timestamp, log);
        // update counters
        return this.ds.batch([
            ['incrby', generateCounter(params, 'storageUtilizedCounter'),
                storageUtilizedDelta],
            numberOfObjectsCounter,
            ['incrby', generateKey(params, 'incomingBytes', timestamp),
                newByteLength],
            ['incr', generateKey(params, 'putObject', timestamp)],
        ], (err, results) => {
            if (err) {
                log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            const cmds = [];
            let actionErr;
            let actionCounter;
            // storage utilized counter
            actionErr = results[0][0];
            actionCounter = results[0][1];
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    metric: 'storage utilized',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            cmds.push(
                ['zremrangebyscore',
                    generateStateKey(params, 'storageUtilized'),
                    timestamp, timestamp],
                ['zadd', generateStateKey(params, 'storageUtilized'),
                    timestamp, actionCounter]);

            // number of objects counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    metric: 'number of objects',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            cmds.push(
                ['zremrangebyscore',
                    generateStateKey(params, 'numberOfObjects'),
                    timestamp, timestamp],
                ['zadd', generateStateKey(params, 'numberOfObjects'),
                    timestamp, actionCounter]);
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for CopyObject action on an object in a Bucket resource.
    * @param {object} params - params for the metrics
    * @param {string} params.bucket - bucket name
    * @param {string} params.account - account ID
    * @param {number} params.newByteLength - size of object in bytes
    * @param {number} params.oldByteLength - previous size of object in bytes
    * if this action overwrote an existing object
    * @param {number} timestamp - normalized timestamp of current time
    * @param {object} log - Werelogs request logger
    * @param {callback} callback - callback to call
    * @return {undefined}
    */
    _pushMetricCopyObject(params, timestamp, log, callback) {
        this._checkTypes(params, ['newByteLength', 'oldByteLength']);
        const { newByteLength, oldByteLength } = params;
        let numberOfObjectsCounter;
        // if previous object size is null then it's a new object in a bucket
        // or else it's an old object being overwritten
        if (oldByteLength === null) {
            numberOfObjectsCounter = ['incr', generateCounter(params,
                'numberOfObjectsCounter')];
        } else {
            numberOfObjectsCounter = ['get', generateCounter(params,
                'numberOfObjectsCounter')];
        }
        const oldObjSize = oldByteLength === null ? 0 : oldByteLength;
        const storageUtilizedDelta = newByteLength - oldObjSize;
        this._logMetric(params, '_pushMetricCopyObject', timestamp, log);
        // update counters
        return this.ds.batch([
            ['incrby', generateCounter(params, 'storageUtilizedCounter'),
                storageUtilizedDelta],
            numberOfObjectsCounter,
            ['incr', generateKey(params, 'copyObject', timestamp)],
        ], (err, results) => {
            if (err) {
                log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricCopyObject',
                    error: err,
                });
                return callback(errors.InternalError);
            }
            const cmds = [];
            let actionErr;
            let actionCounter;
            // storage utilized counter
            actionErr = results[0][0];
            actionCounter = results[0][1];
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricCopyObject',
                    metric: 'storage utilized',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            cmds.push(
                ['zremrangebyscore',
                    generateStateKey(params, 'storageUtilized'),
                    timestamp, timestamp],
                ['zadd', generateStateKey(params, 'storageUtilized'),
                    timestamp, actionCounter]);

            // number of objects counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricCopyObject',
                    metric: 'number of objects',
                    error: actionErr,
                });
                return callback(errors.InternalError);
            }
            cmds.push(
                ['zremrangebyscore',
                    generateStateKey(params, 'numberOfObjects'),
                    timestamp, timestamp],
                ['zadd', generateStateKey(params, 'numberOfObjects'),
                    timestamp, actionCounter]);
            return this.ds.batch(cmds, callback);
        });
    }
}
