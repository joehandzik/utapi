import assert from 'assert';
import { mapSeries, series } from 'async';
import UtapiClient from '../../src/lib/UtapiClient';
import MemoryBackend from '../../src/lib/backend/Memory';
import Datastore from '../../src/lib/Datastore';
import { getBucketCounters, getMetricFromKey } from '../../src/lib/schema';
const testBucket = 'foo';
const memBackend = new MemoryBackend();
const datastore = new Datastore();
const utapiClient = new UtapiClient();
const reqUid = 'foo';
datastore.setClient(memBackend);
utapiClient.setDataStore(datastore);

function _assertCounters(bucket, cb) {
    const counters = getBucketCounters(testBucket);
    return mapSeries(counters, (item, next) =>
        memBackend.get(item, (err, res) => {
            if (err) {
                return next(err);
            }
            const metric = getMetricFromKey(item, bucket)
                .replace(':counter', '');
            assert.equal(res, 0, `${metric} must be 0`);
            return next();
        }), cb);
}

describe('Counters', () => {
    afterEach(() => memBackend.flushDb());

    it('should set counters to 0 on bucket creation', done => {
        utapiClient.pushMetric('createBucket', reqUid, { bucket: testBucket },
            () => _assertCounters(testBucket, done));
    });

    it('should reset counters on bucket re-creation', done => {
        series([
            next => utapiClient.pushMetric('createBucket', reqUid, {
                bucket: testBucket,
            }, next),
            next => utapiClient.pushMetric('listBucket', reqUid, {
                bucket: testBucket,
            }, next),
            next => utapiClient.pushMetric('putObject', reqUid, {
                bucket: testBucket,
                newByteLength: 8,
                oldByteLength: 0,
            }, next),
            next => utapiClient.pushMetric('getObject', reqUid, {
                bucket: testBucket,
                newByteLength: 8,
            }, next),
            next => utapiClient.pushMetric('deleteObject', reqUid, {
                bucket: testBucket,
                newByteLength: 8,
                numberOfObjects: 1,
            }, next),
            next => utapiClient.pushMetric('deleteBucket', reqUid, {
                bucket: testBucket,
            }, next),
            next => utapiClient.pushMetric('createBucket', reqUid, {
                bucket: testBucket,
            }, next),
        ], () => _assertCounters(testBucket, done));
    });
});
