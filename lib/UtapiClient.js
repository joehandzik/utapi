import { Logger } from 'werelogs';
import Datastore from './Datastore';
import { genBucketKey } from './schema';
import redisClient from '../utils/redisClient';

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

    /*
    * Utility function to use when callback is not defined
    */
    _noop() {}

    /**
    * Updates counter for CreateBucket action on a Bucket resource. Since create
    * bucket occcurs only once in a bucket's lifetime, counter is  always 1
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricCreateBucket(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricCreateBucket',
            bucket, timestamp,
        });
        return this.ds.incr(genBucketKey(bucket, 'createBucketCounter'),
            (err, count) => {
                if (err) {
                    this.log.error('error incrementing counter', {
                        method: 'Buckets.pushMetricCreateBucket',
                        error: err,
                    });
                    return cb(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'createBucket'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for DeleteBucket action on a Bucket resource. Since delete
    * bucket occcurs only once in a bucket's lifetime, counter is  always 1
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricDeleteBucket(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricDeleteBucket',
            bucket, timestamp,
        });
        // clear global counters and log the action
        return this.ds.batch([
             ['set', genBucketKey(bucket, 'storageUtilizedCounter'), 0],
             ['set', genBucketKey(bucket, 'incomingBytesCounter'), 0],
             ['set', genBucketKey(bucket, 'outgoingBytesCounter'), 0],
             ['set', genBucketKey(bucket, 'numberOfObjectsCounter'), 0],
             ['incr', genBucketKey(bucket, 'deleteBucketCounter')],
        ], err => {
            if (err) {
                this.log.error('error incrementing counter', {
                    method: 'Buckets.pushMetricDeleteBucket',
                    error: err,
                });
                return callback(err);
            }
            // results format [[null, '1'], [null, '5'], [null, '9']...]
            return this.ds.zadd(genBucketKey(bucket, 'deleteBucket'), timestamp,
                1, callback);
        });
    }


    /**
    * Updates counter for ListBucket action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricListBucket(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricListBucket',
            bucket, timestamp,
        });
        return this.ds.incr(genBucketKey(bucket, 'listBucketCounter'),
            (err, count) => {
                if (err) {
                    this.log.error('error incrementing counter', {
                        method: 'Buckets.pushMetricListBucket',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'listBucket'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for GetBucketAcl action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricGetBucketAcl(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', { method: 'UtapiClient.pushMetricGet' +
            'BucketAcl',
            bucket, timestamp });
        return this.ds.incr(genBucketKey(bucket, 'getBucketAclCounter'),
            (err, count) => {
                if (err) {
                    this.log.error('error incrementing counter', {
                        method: 'Buckets.pushMetricGetBucketAcl',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'getBucketAcl'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for PutBucketAcl action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricPutBucketAcl(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricPutBucketAcl', bucket, timestamp });
        return this.ds.incr(genBucketKey(bucket, 'putBucketAclCounter'),
            (err, count) => {
                if (err) {
                    this.log.error('error incrementing counter', {
                        method: 'Buckets.pushMetricPutBucketAcl',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'putBucketAcl'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for UploadPart action on an object in a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {number} objectSize - size of object in bytes
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricUploadPart(bucket, timestamp, objectSize, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricUploadPart', bucket, timestamp });
        // update counters
        return this.ds.batch([
            ['incrby', genBucketKey(bucket, 'storageUtilizedCounter'),
                objectSize],
            ['incrby', genBucketKey(bucket, 'incomingBytesCounter'),
                objectSize],
            ['incr', genBucketKey(bucket, 'uploadPartCounter')],
        ], (err, results) => {
            if (err) {
                this.log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricUploadPart',
                    error: err,
                });
                return callback(err);
            }
            const cmds = [];
            let actionErr;
            let actionCounter;
            // storage utilized counter
            actionErr = results[0][0];
            actionCounter = results[0][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricUploadPart',
                    metric: 'storage utilized',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'storageUtilized'), timestamp,
                        actionCounter]);
            }

            // incoming bytes counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricUploadPart',
                    metric: 'incoming bytes',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'incomingBytes'),
                    timestamp, actionCounter]);
            }
            // uploadPart counter
            actionErr = results[2][0];
            actionCounter = results[2][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricUploadPart',
                    metric: 'uploadPart',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'uploadPart'),
                    timestamp, actionCounter]);
            }
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for Initiate Multipart Upload action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricInitiateMultipartUpload(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricInitiateMultipartUpload',
            bucket, timestamp,
        });
        return this.ds.incr(
            genBucketKey(bucket, 'initiateMultipartUploadCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricInitiateMultipartUpload',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(
                    genBucketKey(bucket, 'initiateMultipartUpload'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for Complete Multipart Upload action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricCompleteMultipartUpload(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricCompleteMultipartUpload',
            bucket, timestamp,
        });
        return this.ds.batch([
            ['incr', genBucketKey(bucket, 'numberOfObjectsCounter')],
            ['incr', genBucketKey(bucket, 'completeMultipartUploadCounter')],
        ], (err, results) => {
            // number of objects counter
            if (err) {
                this.log.error('error incrementing counter for push metric',
                    {
                        method: 'UtapiClient.pushMetricPutObject',
                        metric: 'number of objects',
                        error: err,
                    });
                return callback(err);
            }
            const cmds = [];
            let actionErr;
            let actionCounter;
            // storage utilized counter
            actionErr = results[0][0];
            actionCounter = results[0][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricCompleteMultipartUpload',
                    metric: 'number of objects',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'numberOfObjects'),
                    timestamp, actionCounter]);
            }

            // completeMultipartUpload counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricCompleteMultipartUpload',
                    metric: 'number of objects',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'completeMultipartUpload'),
                    timestamp, actionCounter]);
            }
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for ListMultipartUploads action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricListBucketMultipartUploads(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricListBucketMultipartUploads',
            bucket, timestamp,
        });
        return this.ds.incr(
            genBucketKey(bucket, 'listBucketMultipartUploadsCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricListBucketMultipart' +
                            'Uploads',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(
                    genBucketKey(bucket, 'listBucketMultipartUploads'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for ListMultipartUploadParts action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricListMultipartUploadParts(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricListMultipartUploadParts',
            bucket, timestamp,
        });
        return this.ds.incr(
            genBucketKey(bucket, 'listMultipartUploadPartsCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricListMultipartUpload' +
                            'Parts',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(
                    genBucketKey(bucket, 'listMultipartUploadParts'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for AbortMultipartUpload action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricAbortMultipartUpload(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricAbortMultipartUpload',
            bucket, timestamp,
        });
        return this.ds.incr(
            genBucketKey(bucket, 'abortMultipartUploadCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricAbortMultipartUpload',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(
                    genBucketKey(bucket, 'abortMultipartUpload'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for DeleteObject action on an object of Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {number} objectSize - size of the object
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricDeleteObject(bucket, timestamp, objectSize, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricDeleteObject',
            bucket, timestamp,
        });
        return this.ds.batch([
            ['decrby', genBucketKey(bucket, 'storageUtilizedCounter'),
                objectSize],
            ['incr', genBucketKey(bucket, 'deleteObjectCounter')],
            ['decr', genBucketKey(bucket, 'numberOfObjectsCounter')],
        ], (err, results) => {
            if (err || results[0][0]) {
                this.log('error incrementing counter', {
                    method: 'UtapiClient.pushMetricDeleteObject',
                    error: err,
                });
                return callback(err);
            }

            const cmds = [];
            // storage utilized counter
            let actionErr = results[0][0];
            let tempCounter = parseInt(results[0][1], 10);
            tempCounter = tempCounter < 0 ? 0 : tempCounter;
            let actionCounter = parseInt(results[0][1], 10);
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricDeleteObject',
                    metric: 'storage utilized',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'storageUtilized'),
                    timestamp, tempCounter]);
            }

            // del object counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricDeleteObject',
                    metric: 'delete object',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'deleteObject'),
                 timestamp, actionCounter]);
            }

            // num of objects counter
            actionErr = results[2][0];
            tempCounter = parseInt(results[2][1], 10);
            tempCounter = tempCounter < 0 ? 0 : tempCounter;
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricDeleteObject',
                    metric: 'num of objects',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'numberOfObjects'),
                 timestamp, tempCounter]);
            }
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for GetObject action on an object in a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {number} objectSize - size of object in bytes
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricGetObject(bucket, timestamp, objectSize, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricGetObject', bucket, timestamp });
        // update counters
        return this.ds.batch([
            ['incrby', genBucketKey(bucket, 'outgoingBytesCounter'),
            objectSize],
            ['incr', genBucketKey(bucket, 'outgoingBytesCounter')],
        ], (err, results) => {
            if (err) {
                this.log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricGetObject',
                    error: err,
                });
                return callback(err);
            }
            const cmds = [];
            // storage utilized counter
            let actionErr = results[0][0];
            let actionCounter = results[0][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricGetObject',
                    metric: 'outgoing bytes',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'outgoingBytes'),
                    timestamp, actionCounter]);
            }

            // get object counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricGetObject',
                    metric: 'outgoing bytes',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'getObject'), timestamp,
                    actionCounter]);
            }
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for GetObjectAcl action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricGetObjectAcl(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricGetObjectAcl',
            bucket, timestamp,
        });
        return this.ds.incr(genBucketKey(bucket, 'getObjectAclCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricGetObjectAcl',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'getObjectAcl'),
                    timestamp, count, callback);
            });
    }


    /**
    * Updates counter for PutObject action on an object in a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {number} objectSize - size of object in bytes
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricPutObject(bucket, timestamp, objectSize, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric',
            { method: 'UtapiClient.pushMetricPutObject', bucket, timestamp });
        // update counters
        return this.ds.batch([
            ['incrby', genBucketKey(bucket, 'storageUtilizedCounter'),
                objectSize],
            ['incrby', genBucketKey(bucket, 'incomingBytesCounter'),
                objectSize],
            ['incr', genBucketKey(bucket, 'numberOfObjectsCounter')],
            ['incr', genBucketKey(bucket, 'putObjectCounter')],
        ], (err, results) => {
            if (err) {
                this.log.error('error pushing metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    error: err,
                });
                return callback(err);
            }
            const cmds = [];
            let actionErr;
            let actionCounter;
            // storage utilized counter
            actionErr = results[0][0];
            actionCounter = results[0][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    metric: 'storage utilized',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'storageUtilized'),
                    timestamp, actionCounter]);
            }

            // incoming bytes counter
            actionErr = results[1][0];
            actionCounter = results[1][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    metric: 'incoming bytes',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'incomingBytes'),
                    timestamp, actionCounter]);
            }

            // number of objects counter
            actionErr = results[2][0];
            actionCounter = results[2][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    metric: 'number of objects',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd',
                    genBucketKey(bucket, 'numberOfObjects'), timestamp,
                    actionCounter]);
            }

            // putObject counter
            actionErr = results[3][0];
            actionCounter = results[3][1];
            if (actionErr) {
                this.log.error('error incrementing counter for push metric', {
                    method: 'UtapiClient.pushMetricPutObject',
                    metric: 'put object',
                    error: actionErr,
                });
            } else {
                cmds.push(['zadd', genBucketKey(bucket, 'putObject'), timestamp,
                    actionCounter]);
            }
            return this.ds.batch(cmds, callback);
        });
    }

    /**
    * Updates counter for PutObjectAcl action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricPutObjectAcl(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricPutObjectAcl',
            bucket, timestamp,
        });
        return this.ds.incr(genBucketKey(bucket, 'putObjectAclCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricPutObjectAcl',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'putObjectAcl'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for HeadBucket action on a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricHeadBucket(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricHeadBucket',
            bucket, timestamp,
        });
        return this.ds.incr(genBucketKey(bucket, 'headBucketCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricHeadBucket',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'headBucket'),
                    timestamp, count, callback);
            });
    }

    /**
    * Updates counter for HeadObject action on an object in a Bucket resource.
    * @param {string} bucket - bucket name
    * @param {number} timestamp - unix epoch timestamp
    * @param {callback} [cb] - (optional) callback to call
    * @return {undefined}
    */
    pushMetricHeadObject(bucket, timestamp, cb) {
        const callback = cb || this._noop;
        if (this.disableClient) {
            return callback();
        }
        this.log.trace('pushing metric', {
            method: 'UtapiClient.pushMetricHeadObject',
            bucket, timestamp,
        });
        return this.ds.incr(genBucketKey(bucket, 'headObjectCounter'),
            (err, count) => {
                if (err) {
                    this.log('error incrementing counter', {
                        method: 'UtapiClient.pushMetricHeadObject',
                        error: err,
                    });
                    return callback(err);
                }
                return this.ds.zadd(genBucketKey(bucket, 'headObject'),
                    timestamp, count, callback);
            });
    }
}