#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
import json
import boto3
import datetime
import elasticsearch
import requests_aws4auth


ES_INFO = [
    {
        "ES_HOST": 'search-xxxxxxxxxx1.us-west-2.es.amazonaws.com',
        "REGION": 'us-west-2',
        "SERVICE": 'es'
    },
    {
        "ES_HOST": 'search-xxxxxxxxxxx2.us-west-2.es.amazonaws.com',
        "REGION": 'us-west-2',
        "SERVICE": 'es'
    }

]

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

def es_authentication(service, region, es_host):
    credentials = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY).get_credentials()
    aws_auth = requests_aws4auth.AWS4Auth(credentials.access_key, credentials.secret_key,
                                          region, service, session_token=credentials.token)
    es = elasticsearch.Elasticsearch(
        hosts=[{'host': es_host, 'port': 443}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=elasticsearch.RequestsHttpConnection,
        timeout=30
    )
    return es


def get_date():
    today = datetime.datetime.now().strftime("%Y.%m.%d")
    next_day = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime("%Y.%m.%d")
    return today, next_day


def get_indices_list(today):
    indices_list = []
    res = es_client.cat.indices('*-'+str(today), format='json')
    [indices_list.append(i["index"]) for i in res]
    return indices_list


def get_index_template(index_name):
    index_template = es_client.search(index=index_name, filter_path=['hits.hits._source'], )
    return index_template['hits']['hits'][0]['_source']


def get_doctype(index_name):
    """doctype in older version es can be define by user self, but must be _doc in newer es version."""
    doctype = es_client.search(index=index_name, filter_path=['hits.hits._type'])
    return doctype['hits']['hits'][0]['_type']


def create_new_index(indices_list):
    for name in indices_list:
        index_template, doctype = get_index_template(name), get_doctype(name)
        new_index_name = name[:-10] + get_date()[1]
        print("Starting create index %s" % new_index_name)
        time.sleep(3)
        es_client.index(index=new_index_name, doc_type=doctype, body=json.dumps(index_template))


if __name__ == "__main__":
    for each_es in ES_INFO:
        es_client = es_authentication(each_es['SERVICE'], each_es['REGION'], each_es['ES_HOST'])
        indices_name_list = get_indices_list(get_date()[0])
        create_new_index(indices_name_list)

