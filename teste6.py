# Databricks notebook source
from pyspark.sql.functions import *
import requests
import json
from tqdm import tqdm
import requests
import json

# COMMAND ----------
def _get_token(url_base, login, password):
    credentials = {
        "login": login,
        "password": password
    }
    response = requests.post(
        url_base + '/service/integration-authorization-service.login',
        data=json.dumps(credentials)
    )
    if response.status_code == 200:
        response = response.json()
        return {'x-authorization-user-id': response.get('userId'),
              'x-authorization-token': response.get('key')}
    else:
        raise Exception(f"Check credentials, returned status code {response.status_code}")


def _get_query(params):
    return '&'.join([f'{k}={v}' for k, v in params.items()])


def _choise_file_type(response):
    if response.headers['Content-Type'] == 'application/gzip':
        return '.csv.gz'
    elif response.headers['Content-Type'] == 'text/csv':
        return '.csv'
    else:
        raise Exception(f"Unmapped file type {response.headers['Content-Type']} \n response content: {response.text}")


def get_file(url_base, login, password, cube_id):
    params = {
        "cube": '{"id":"%s"}' % cube_id,
        "charset": "UTF-8",
        "delimiter": ";",
        "quote": '"',
        "escape": "\\"
    }
    query = _get_query(params)
    authorizations = _get_token(url_base, login, password)
    response = requests.get(
        f"{url_base}service/integration-cube-service.download?{query}",
        stream=True,
        headers=authorizations
    )
    with open(f'/dbfs/tmp/{cube_id}{_choise_file_type(response)}', 'wb') as file:
        for chunk in tqdm(response.iter_content(chunk_size=20000024)):
            if chunk:
                file.write(chunk)

# COMMAND ----------
url_base = 'https://salesdata.cortex-intelligence.com/'
get_file(url_base, 'dataplatform', 'CH2hbas5EtNigts', 'dee047598beb4f1b9aa81afd19e04630')