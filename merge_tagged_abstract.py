import argparse
from tqdm import tqdm
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import scan
import re
import warnings

warnings.filterwarnings("ignore")

from_index = 'pubmed-18-taggedtext'
to_index = 'pubmed-18'

## Converts entity attributes to classes
def from_entity_to_class(t):
    rex = re.compile(r"class=\"(?P<mark>mark-\d+)\"(?P<rest>.+?)data-entity=\"(?P<entity>[A-Z]+&?[A-Z]+)\"")
    # rex1 = re.compile(r"data-entity=\"(?P<entity>[A-Z]+&?[A-Z]+)\"")
    #entityMatch = re.search(rex1, t)
    #print(entityMatch.group('entity'))

    t1 = rex.sub("\g<rest> class=\"\g<mark> \g<entity>\" data-identity=\"\g<entity>\"", t)

    #rex2 = re.compile(r"class=\"(?P<mark>mark-\d+)\"")
    #t = rex2.sub('class=\"\g<mark> ' + re.escape(entityMatch.group('entity')) + '\"', t)
    return t1


# Converts from elements to classes in the tagged abstract
def from_element_to_class(t):
    reg_start = re.compile(r"<(?P<mark>mark-\d+)")
    reg_end = re.compile(r"</mark-\d+>")
    t1 = reg_start.sub("<span class=\"\g<mark>\"", t)
    t2 = reg_end.sub("</span>", t1)
    return t2


def get_number_of_records(es):
    query = {
        'query': {}
    }

    r = es.count(index=from_index)
    return r['count']


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-es', dest='es', action='append',
                        default=[],
                        help='elasticsearch urls')
    parser.add_argument('-username', required=False, default='admin')
    parser.add_argument('-password', required=False, default='password')
    args = parser.parse_args()

    ES_AUTH = (args.username, args.password)

    es = Elasticsearch(
        hosts=args.es,
        max_retry=10,
        retry_on_timeout=True,

        use_ssl=True,
        verify_certs=False,
        http_auth=ES_AUTH,
        connection_class=RequestsHttpConnection
    )

    read_records = 0
    c = get_number_of_records(es)
    with tqdm(
        desc='loading formatted text jsons',
        unit=' docs',
        unit_scale=True,
        total=c
    ) as pbar:
        for item in scan(client=es,
                             query={
                                 'query': {}
                             },
                             scroll='5m',
                             raise_on_error=True,
                             preserve_order=False,
                             size=100,
                             index=from_index):
            read_records += 1
            pbar.update(1)

            tagged_abstract = item['_source']['abstract']
            tagged_abstract_classes = from_element_to_class(tagged_abstract)
            tagged_abstract_classes_plus = from_entity_to_class(tagged_abstract_classes)
            doc_id = item['_id']
            doc_type = item['_type']

            # retrieve the same record from the destination index
            # rec_to = es.get(index=to_index,
            #                 id=doc_id,
            #                 _source=True)
            # rec_to['tagged_abstract'] = tagged_abstract
            # print(rec_to)
            es.update(index=to_index,
                      id=doc_id,
                      doc_type='publication',
                      body={
                          'doc': {
                              'abstract_tagged': tagged_abstract,
                              # 'abstract_classes': tagged_abstract_classes,
                              # 'abstract_classes_plus': tagged_abstract_classes_plus
                          }
                      })
