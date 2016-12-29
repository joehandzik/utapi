import Accounts from '../lib/Account';
/**
@class AccountsHandler
* Handles Accounts resource actions
*/

export default class AccountsHandler {

    /**
    * List metrics for the given list of buckets
    * @param {UtapiRequest} utapiRequest - UtapiRequest instance
    * @param {callback} cb - callback
    * @return {undefined}
    */
    static listMetrics(utapiRequest, cb) {
        const log = utapiRequest.getLog();
        log.debug('handling list metrics request', {
            method: 'AccountsHandler.listMetrics',
        });
        return Accounts.getTypesMetrics(utapiRequest, cb);
    }
}
