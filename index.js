require('dotenv').config();

const AXIOS = require('axios');
const DAYJS = require('dayjs')
const AWS = require('aws-sdk');

const DEBUG = false;

AWS.config.update({
    "region": process.env.AWS_REGION,
    "accessKeyId": process.env.AWS_ACCESS_KEY,
    "secretAccessKey": process.env.AWS_ACCESS_KEY_SECRET
});

// AWS Services
const ssmClient = new AWS.SSM();
let docClient = new AWS.DynamoDB.DocumentClient();

let ENDPOINT_01 ;
let ENDPOINT_02 ;
let DATA_TABLE ;
let DATA_TABLE_PRICES ;

let clientsList = [];
let clientsListFiltered = [];

const startDate = DAYJS();

getAWSParameters('fuelpriceguide')
    .then( parametersKeys =>{
        if (DEBUG) console.log(JSON.stringify(parametersKeys));

        // define aplication parameters
        DATA_TABLE = parametersKeys[0].Value;
        DATA_TABLE_PRICES = parametersKeys[1].Value;
        ENDPOINT_01 = parametersKeys[2].Value;
        ENDPOINT_02 = parametersKeys[3].Value;
    })
    .then( () => {
        // fetch all fuel data
        getAllData()
            .then( () => {
                // filtered data
                getFilteredData()
                    .then( async () => {
                        // create clients
                        await createClient()
                            .then( () => {
                                if (DEBUG) console.log("db created successfully ..");
                            })
                            .catch( (error) => {
                                if (DEBUG) console.log(error);
                            });

                        const endDate = DAYJS();
                        if (DEBUG) console.log("Time for execution(minutes): " + endDate.diff(startDate, 'minute'));
                    });
                // TODO ENVIAR EMAIL POR AWS SES COM RELATORIO DA IMPORTACAO
            });
    })


async function checkClientPrices(clientPrices, clientItem) {
    // console.log(JSON.stringify(clientPrices));
    console.log(JSON.stringify(clientItem));

    const key = 'DataAtualizacao';

    // get unique objects by key from price array
    const uniqueClientPrices = [...new Map(clientPrices.Items[0].Combustiveis.map(item =>
        [item[key], item])).values()];

    // get unique objects by key from price current item array
    const uniqueClientItemPrices = [...new Map(clientItem[0].Combustiveis.map(item =>
        [item[key], item])).values()];

    // console.log(uniqueClientPrices);
    // console.log(JSON.stringify(uniqueClientItemPrices));
}

async function getAllData() {

    // first get update list of clients and build temporary clients list
    await AXIOS.get(ENDPOINT_01)
        .then((response) => {
            //console.log(response.data);
            response.data.resultado.forEach(clientRaw => {
                let client = {
                    id: clientRaw.Id,
                    nome: clientRaw.Nome
                }
                clientsList.push(client);
            })
            if (DEBUG) console.log("List raw: " + clientsList.length);
        })
        .catch(error => {
            // handle error
            if (DEBUG) console.log(error);
        });
}

/** Resolves if filtered client lis of clients is successfully executed
 *  Rejects if something wrong happens in this data process
 *
 *  - Rejects with 500 - if something wrong happens putting in the dynamo
 *
 * @returns {Promise<unknown>}
 */
async function getFilteredData() {

    // go through client list and get individual data
    for (const clientRow of clientsList) {

        await AXIOS.get(ENDPOINT_02 + clientRow.id)
            .then((response) => {
                //console.log(response2);

                if (response.data.resultado.Nome != null &&
                    response.data.resultado.Morada != null &&
                    response.data.resultado.Combustiveis != null) {

                    //console.log(response.data);
                    let client = {
                        Codigo: clientRow.id,
                        Nome: clientRow.nome,
                        Marca: response.data.resultado.Marca,
                        Utilizacao: response.data.resultado.Utilizacao,
                        Morada: response.data.resultado.Morada,
                        HorarioPosto: response.data.resultado.HorarioPosto,
                        Servicos: response.data.resultado.Servicos,
                        MeiosPagamento: response.data.resultado.MeiosPagamento,
                        Combustiveis: response.data.resultado.Combustiveis,
                    }
                    clientsListFiltered.push(client);
                }
            })
            .catch(error => {
                // handle error
                if (DEBUG) console.log(error);
            });
    }

    if (DEBUG) console.log("List filtered: " + clientsListFiltered.length);
}

/** Resolves if creation of client on dynamo is successfully executed
 *  Rejects if something wrong happens in this data process
 *
 *  - Rejects with 500 - if something wrong happens putting in the dynamo
 *
 * @returns {Promise<unknown>}
 */
async function createClient() {
    // go through client list and get individual data
    for (const clientRow of clientsListFiltered) {

        await createDynamoClient(clientRow)
            .then( async () => {
                if (DEBUG) console.log("Client created successfully ..");
            })
            .catch( (error) => {
                if (DEBUG) console.log(error);
            });

    }

    if (DEBUG) console.log("Clients added: " + clientsListFiltered.length);
}

/** Resolves if creation of client on dynamo is successfully executed
 *  Rejects if something wrong happens in this data process
 *
 *  - Rejects with 500 - if something wrong happens putting in the dynamo
 *
 * @param {Object} clientItem
 * @returns {Promise<unknown>}
 */
function createDynamoClientPrice(clientItem, ) {
    return new Promise(async(resolve, reject) => {
        if (DEBUG) console.log('-> Creating client price in dynamo');

        let params = await buildCreatePriceParams(clientItem);

        docClient.put(params).promise()
            .then( data => {
                if (DEBUG) console.log(`Client price created successfully..`);
                return resolve(data);
            })
            .catch(err => {
                // Internal error -> rejects with 500
                // let response = API.buildResponse(API.RESPONSE.INTERNAL_SERVER_ERROR, globalContext);
                if (DEBUG) console.log(err);

                return reject({
                    errorResponse: err.errorResponse,
                    errorMessage: err
                });
            });
    });
}

/** Resolves if query of client price on dynamo is successfully executed
 *  Rejects if something wrong happens in this data process
 *
 *  - Rejects with 500 - if something wrong happens putting in the dynamo
 *
 * @param {Object} clientItem
 * @returns {Promise<unknown>}
 */
function queryDynamoClientPrice(clientItem, ) {
    return new Promise(async(resolve, reject) => {
        if (DEBUG) console.log('-> Query client price in dynamo');

        let params = await buildQueryPriceParams(clientItem);

        docClient.query(params).promise()
            .then( data => {
                if (DEBUG) console.log(`Client price retrieved successfully..`);
                return resolve(data);
            })
            .catch(err => {
                // Internal error -> rejects with 500
                // let response = API.buildResponse(API.RESPONSE.INTERNAL_SERVER_ERROR, globalContext);
                if (DEBUG) console.log(err);

                return reject({
                    errorResponse: err.errorResponse,
                    errorMessage: err
                });
            });
    });
}

/** Resolves if creation of client price on dynamo is successfully executed
 *  Rejects if something wrong happens in this data process
 *
 *  - Rejects with 500 - if something wrong happens putting in the dynamo
 *
 * @param {Object} clientItem
 * @returns {Promise<unknown>}
 */
function createDynamoClient(clientItem, ) {
    return new Promise(async(resolve, reject) => {
        if (DEBUG) console.log('-> Creating client in dynamo');

        let params = await buildCreateParams(clientItem);

        // create client details
        docClient.put(params).promise()
            .then( async data => {
                if (DEBUG) console.log(`Client created successfully..`);
                if (DEBUG) console.log(`Client created data: ` + JSON.stringify(data));

                // create price list for client
                await createDynamoClientPrice(clientItem)
                    .then(() => {
                        if (DEBUG) console.log("Client price created successfully ..");
                    })
                    .catch((error) => {
                        if (DEBUG) console.log(error);
                    });

                return resolve(data);
            })
            .catch(async err => {

                // Internal error -> 'ConditionalCheckFailedException:The conditional request failed at Request'
                // Client id allready exists just update prices
                if (err.message === 'The conditional request failed at Request' || err.code === 'ConditionalCheckFailedException') {


                    // TODO Validate current price list with last db imported price list and get last item from bd to check last update datetime
                    // create price list for client
                    await createDynamoClientPrice(clientItem)
                        .then(() => {
                            if (DEBUG) console.log("Client price updated successfully ..");
                        })
                        .catch((error) => {
                            if (DEBUG) console.log(error);
                        });

                    return resolve();
                }

                // Internal error -> rejects with 500
                if (DEBUG) console.log(err);

                return reject({
                    errorResponse: err.message,
                    errorMessage: err
                });
            });
    });
}

/** Resolves always a params object to be used in dynamoPut.
 *
 * @param {Object} clientItem - Contains the identifier from queryString parameter.
 *
 */
function buildQueryPriceParams(clientItem, ) {
    return new Promise((resolve, reject) => {
        let params = {
            TableName: DATA_TABLE_PRICES,
            KeyConditionExpression: 'Id = :v_ID AND #v_timestamp <= :v_timestamp',
            ExpressionAttributeNames: {
                "#v_timestamp": "Timestamp"
            },
            ExpressionAttributeValues: {
                ":v_ID": clientItem.Codigo,
                ":v_timestamp": DAYJS().format('YYYY-MM-DD HH:mm:ss'),
            },
            ScanIndexForward: false, //DESC ORDER, Set 'true' if asc order
            Limit: 1,
        };

        if (DEBUG) console.log('PARAMS: ', params);

        return resolve(params);
    });
}


/** Resolves always a params object to be used in dynamoPut.
 *
 * @param {Object} clientItem - Contains the identifier from queryString parameter.
 *
 */
function buildCreatePriceParams(clientItem, ) {
    return new Promise((resolve, reject) => {
        let params = {
            TableName: DATA_TABLE_PRICES,
            Item: {
                Id: clientItem.Codigo,
                Combustiveis: clientItem.Combustiveis,
                Timestamp: DAYJS().format('YYYY-MM-DD HH:mm:ss'),
            },
            // ConditionExpression: 'attribute_not_exists(Id)', // only create new account if it does not exist
        };

        if (DEBUG) console.log('PARAMS: ', params);

        return resolve(params);
    });
}

/** Resolves always a params object to be used in dynamoPut.
 *
 * @param {Object} clientItem - Contains the identifier from queryString parameter.
 *
 */
function buildCreateParams(clientItem, ) {
    return new Promise((resolve, reject) => {
        let params = {
            TableName: DATA_TABLE,
            Item: {
                Id: clientItem.Codigo,
                Nome: clientItem.Nome,
                Marca: clientItem.Marca,
                Morada: clientItem.Morada,
                HorarioPosto: clientItem.HorarioPosto,
                Servicos: clientItem.Servicos,
                MeiosPagamento: clientItem.MeiosPagamento,
                CreateTimestamp: DAYJS().format('YYYY-MM-DD HH:mm:ss'),
                UpdateTimestamp: DAYJS().format('YYYY-MM-DD HH:mm:ss'),

            },
            ConditionExpression: 'attribute_not_exists(Id)', // only create new account if it does not exist
        };

        if (DEBUG) console.log('PARAMS: ', params);

        return resolve(params);
    });
}

/** Resolves if retrieval of aws store parameters is successfully executed
 *  Rejects if something wrong happens in this data process
 *
 *  - Rejects with 500 - if something wrong happens retrieving keys
 *
 * @param {String} filterKey app key
 * @returns {Promise<unknown>}
 */
function getAWSParameters(filterKey,) {
    return new Promise(async (resolve, reject) => {
        if (DEBUG) console.log('-> Retrieving keys from aws parameter store');

        const params = {
            Path: `/${filterKey}/`,
            Recursive: true,
            WithDecryption: false
        }

        await ssmClient.getParametersByPath(params).promise()
            .then(data => {
                if (DEBUG) console.log(`AWS Parameters data: ` + JSON.stringify(data));
                return resolve(data.Parameters);
            })
            .catch(err => {
                // Internal error -> rejects with 500
                if (DEBUG) console.log(err);

                return reject({
                    errorResponse: err.message,
                    errorMessage: err
                });
            })
    });
}
