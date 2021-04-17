from datetime import timedelta, date
import json
import os

import requests
import yaml
from requests import HTTPError


class BaseRequest:

    def __init__(self, url):
        self.url = url
        self.header = {"Content-Type": "application/json"}


class AuthRequest(BaseRequest):
    def __init__(self, url, payload):
        super().__init__(url)
        self.payload = payload

    def post(self):
        try:
            response = requests.post(self.url, data=json.dumps(self.payload), headers=self.header)
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            print(f'Other error occurred: {err}')
            return None
        else:
            return response.json()


class DataRequest(BaseRequest):
    def __init__(self, url, payload, access_token):
        super().__init__(url)
        self.payload = payload
        self.access_token = access_token
        self.header['Authorization'] = f"JWT {access_token}"

    def get(self):

        try:
            response = requests.get(self.url, data=json.dumps(self.payload), headers=self.header)
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
            return None
        except Exception as err:
            print(f'Other error occurred: {err}')
            return None
        else:
            return response.json()


def get_auth_payload(config):
    param = config['app']['endpoint_auth']['payload']
    return param


def load_by_partitions(json_object_lists, data_payload):
        dir_name = str(data_payload['date'])
        if not os.path.exists(f"./data/{dir_name}"):
            os.makedirs(f"./data/{dir_name}")

        with open(f"./data/{dir_name}/product.json", mode='a+') as file:
            json.dump(json_object_lists, file)

def load_config(config_path):
    with open(config_path, mode='r') as file_config:
        config = yaml.safe_load(file_config)
        return config

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


if __name__ == '__main__':

    config = load_config("./config/configuration.yaml")

    auth_url = config['app']['url'] + config['app']['endpoint_auth']['endpoint']
    auth_payload = get_auth_payload(config)
    auth_token = AuthRequest(auth_url, auth_payload).post()

    data_url = config['app']['url'] + config['app']['endpoint_data']['endpoint']

    start_date = date(2021, 1, 1)
    end_date = date(2021, 2, 1)

    for single_date in daterange(start_date, end_date):
        print(single_date.strftime("%Y-%m-%d"))
        data_payload = {"date": f'{single_date.strftime("%Y-%m-%d")}'}
        data = DataRequest(data_url, data_payload, auth_token['access_token']).get()
        load_by_partitions(data, data_payload)
