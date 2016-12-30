import Validator from './Validator';

/**
 * Function to create a validator for route POST /?Action=ListMetrics
 * @param {object} dict - Input fields for route
 * @return {Validator} Return the created validator
 */
export default function accountsListMetrics(dict) {
    return new Validator({
        timeRange: true,
        accounts: true,
    }, dict);
}
