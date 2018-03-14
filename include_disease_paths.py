import json
import argparse
import warnings
from tqdm import tqdm
from opentargets import OpenTargetsClient
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import scan

warnings.filterwarnings("ignore")

ES_AUTH = ('demo', 'enterprise123#')


def get_number_of_records(es):
    query = {
        "query": {
            "bool": {
                "must": {
                    "exists": {
                        "field": "text_mined_entities.nlp.tagged_entities_grouped.DISEASE|OPENTARGETS.reference"
                    }
                }
            }
        }
    }

    r = es.count(index='pubmed-18',
                 body=query)
    return r['count']



def get_all_efo_ids(fname):
    ids = {}
    with open(fname) as fp:
        for line in tqdm(fp, desc='extracting all efo ids from OT dumps'):
            assoc = json.loads(line)
            id = assoc['disease']['id']
            ids[id] = True
    return ids.keys()


def get_paths_for_ids(ot, ids):
    paths = {}
    for id in tqdm(ids, desc="retrieving paths for efo ids"):
        res = ot.conn.get('/platform/private/disease/%s' % id)
        labels_joined = ['|'.join(d) for d in res.info['path_labels']]
        codes_joined = ['|'.join(d) for d in res.info['path_codes']]
        paths[id] = {
            'labels': res.info['path_labels'],
            'labels_joined': labels_joined,
            'codes': res.info['path_codes'],
            'codes_joined': codes_joined
        }
    return paths

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Include ontology paths to root for diseases using OT python api")
    parser.add_argument('-es', required=False, default='https://localhost:9220')
    parser.add_argument('-assocs', required=True)
    args = parser.parse_args()

    ot = OpenTargetsClient()
    efo_ids = get_all_efo_ids(args.assocs)
    paths = get_paths_for_ids(ot, efo_ids)


    es = Elasticsearch(
        hosts = args.es,
        max_retry = 10,
        retry_on_timeout = True,
        use_ssl = True,
        verify_certs = False,
        http_auth = ES_AUTH,
        connection_class = RequestsHttpConnection
    )
    c = get_number_of_records(es)

    with tqdm(
        desc='processing abstracts...',
        unit=' docs',
        unit_scale=-True,
        total=c
    ) as pbar:
        for item in scan(client=es,
                         query={
                             '_source': ["text_mined_entities"],
                             "query": {
                                 "bool": {
                                     "must": {
                                         "exists": {
                                             "field": "text_mined_entities.nlp.tagged_entities_grouped.DISEASE|OPENTARGETS.reference"
                                         }
                                     }
                                 }
                             }
                         },
                         scroll='5m',
                         raise_on_error=True,
                         preserve_order=False,
                         size=100,
                         index='pubmed-18'):
            pbar.update(1)
            doc_id = item['_id']
            doc_type = item['_type']
            nlp_diseases = item['_source']['text_mined_entities']['nlp']['tagged_entities_grouped']['DISEASE|OPENTARGETS']
            for i, disease in enumerate(nlp_diseases):
                if disease['reference'] == 'EFO_0000249':
                    paths_for_id = paths[disease['reference']]['labels_joined']
                    item['_source']['text_mined_entities']['nlp']['tagged_entities_grouped']['DISEASE|OPENTARGETS'][i]['path'] = paths_for_id
                    es.update(index='pubmed-18',
                              id=doc_id,
                              doc_type=doc_type,
                              body={
                                  'doc': {
                                      'text_mined_entities': item
                                  }
                              })
