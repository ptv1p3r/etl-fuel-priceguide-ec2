require('dotenv').config();
const AXIOS = require('axios');
const DAYJS = require('dayjs')
const AWS = require('aws-sdk');

let clientsList = [];
let clientsListFiltered = [];

const startDate = DAYJS();

getAllData()
    .then( () => {
        getFilteredData()
            .then( () => {
                const endDate = DAYJS();
                console.log("Time for execution(minutes): " + endDate.diff(startDate,'minute'));
            });
    });

async function getAllData() {
// first get update list of clients and build temporary clients list
    await AXIOS.get('https://precoscombustiveis.dgeg.gov.pt/api/PrecoComb/ListarDadosPostos')
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
            console.log(error);
        });
}

async function getFilteredData() {
    // go through client list and get individual data
    for (const clientRow of clientsList) {

        await AXIOS.get('https://precoscombustiveis.dgeg.gov.pt/api/PrecoComb/GetDadosposto?Id=' + clientRow.id)
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
                console.log(error);
            });
    }

    console.log("List filtered: " + clientsListFiltered.length);
}
