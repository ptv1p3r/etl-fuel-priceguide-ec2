require('dotenv').config();

const AXIOS = require('axios');
const DAYJS = require('dayjs')
const AWS = require('aws-sdk');

// TODO Change env variables to parameter store instead of .env file : https://evanhalley.dev/post/aws-ssm-node/
AWS.config.update({
    "region": process.env.AWS_REGION,
    "accessKeyId": process.env.AWS_ACCESS_KEY,
    "secretAccessKey": process.env.AWS_ACCESS_KEY_SECRET
});

const ENDPOINT_01 = process.env.DBEG_ENDPOINT_1;
const ENDPOINT_02 = process.env.DBEG_ENDPOINT_2;

const DATA_TABLE = process.env.AWS_DYNAMO_TABLE;
const DATA_TABLE_PRICES = process.env.AWS_DYNAMO_TABLE_PRICES;

const DEBUG = false;

let docClient = new AWS.DynamoDB.DocumentClient();

let clientsList = [];
let clientsListFiltered = [];

const startDate = DAYJS();

getAllData()
    .then( () => {
        getFilteredData()
            .then( async () => {

                await createClient()
                    .then( () => {
                        console.log("db created successfully ..");
                    })
                    .catch( (error) => {
                        if (DEBUG) console.log(error);
                    });

                const endDate = DAYJS();
                console.log("Time for execution(minutes): " + endDate.diff(startDate, 'minute'));
            });
    });

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
            console.log("List raw: " + clientsList.length);
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

    console.log("List filtered: " + clientsListFiltered.length);
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
                console.log("Client created successfully ..");
            })
            .catch( (error) => {
                if (DEBUG) console.log(error);
            });

    }

    console.log("Clients added: " + clientsListFiltered.length);
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
        console.log('-> Creating client price in dynamo');

        let params = await buildCreatePriceParams(clientItem);

        docClient.put(params).promise()
            .then( data => {
                console.log(`Client price created successfully..`);
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
        console.log('-> Creating client in dynamo');

        let params = await buildCreateParams(clientItem);

        // create client details
        docClient.put(params).promise()
            .then( async data => {
                console.log(`Client created successfully..`);
                console.log(`Client created data: ` + JSON.stringify(data));

                // create price list for client
                await createDynamoClientPrice(clientItem)
                    .then(() => {
                        console.log("Client price created successfully ..");
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

                    // create price list for client
                    await createDynamoClientPrice(clientItem)
                        .then(() => {
                            console.log("Client price updated successfully ..");
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
