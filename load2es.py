import argparse
import gzip
import json
import logging
import time
import codecs
from functools import partial
from itertools import islice
from multiprocessing.dummy import Pool
from tempfile import NamedTemporaryFile
from fnmatch import fnmatch
import warnings
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import streaming_bulk, parallel_bulk
from google.cloud import storage
from tqdm import tqdm

warnings.filterwarnings('ignore')


'''
tmux new-session "python load2es.py publication --es http://myes:9200"
'''

# load the pubmed ids for chembl:
fname = 'chembl23_pubmed_ids.csv'
chembl_pubmed_ids_file = open(fname, 'r')
chembl_pubmed_ids = chembl_pubmed_ids_file.read().split('\n')
chembl_pubmed_ids_dict = {}
for id in chembl_pubmed_ids:
    chembl_pubmed_ids_dict[id.strip()] = True

#print(chembl_pubmed_ids)
#print(chembl_pubmed_ids_dict)
#exit()

# cache_file = '/tmp/load2es_cache.json'
# INDEX_NAME = 'pubmed-18'
# DOC_TYPE = 'publication'

index_config = {
    'bioentity':
        dict(path='pubmed18-*-of-03935_bioentities.json.gz',
             index='pubmed-18-bioentity',
             doc_type='bioentity',
             mappings=None,
             pub_id = True),
    'taggedtext':
        dict(path='pubmed18-*-of-03935_taggedtext.json.gz',
             index='pubmed-18-taggedtext',
             doc_type='taggedtext',
             mappings=None,
             pub_id = True),
    'publication':
        dict(path='pubmed18-*-of-03935_small.json.gz',
             index='pubmed-18',
             doc_type='publication',
             mappings='publication.json',
             # mappings=None,
             pub_id = True
             ),
    'concept':
        dict(path='pubmed18-*-of-03935_concepts.json.gz',
             index='pubmed-18-concept',
             doc_type='concept',
             mappings='concept.json',
             pub_id = False),

}

#client = storage.Client(project='open-targets')
client = storage.Client(project='siren-pubmed')
#client = storage.Client()
bucket = client.get_bucket('medline-json')

'''
if the data cannot be accessed, make sure this was run
gsutil iam -r ch allUsers:objectViewer gs://medline-json/
'''


def grouper(iterable, size):
    """
    >>> list(grouper( 'ABCDEFG', 3))
    [['A', 'B', 'C'], ['D', 'E', 'F'], ['G']]
    """
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, size)), [])


def get_next_record(reader):
    rec_ok = False
    rec = ''
    while not rec_ok:
        line = reader.next()
        if not line:
            yield None
        rec = "".join([rec, line])
        try:
            line_json = json.loads(rec)
            yield(rec, line_json)
            rec = ''
        except Exception:
            rec = rec + ' '
            pass


def read_remote_file(index_, doc_type, file_name, malformed, use_pub_id = True):
    malformed[file_name] = 0
    in_chembl = 0
    logging.info(file_name)
    counter = 0
    while counter <= 3:  # retry 3 times
        counter += 1
        try:
            with  NamedTemporaryFile() as cache_file:
                blob = bucket.get_blob(file_name)
                blob.chunk_size = 262144 * 4
                blob.download_to_file(cache_file, )
                cache_file.flush()
                zf = gzip.open(cache_file.name, 'rb')
                reader = codecs.getreader("utf-8")
                new_line = []

                for (line, line_json) in get_next_record(reader(zf)):

                #for line in reader(zf):
                    # try:
                    #     line_json = json.loads(line)
                    # except Exception:
                    #     malformed[file_name] += 1
                    #     print(line)
                        # exit()
                        # lines = line.split("\n")
                        # ll = ' '.join(lines)
                        # logging.error(line)
                        # logging.error(ll)

                    # logging.error('comparing %s (type %s) with %s (type %s)' % (
                    #     str(line_json["pub_id"]),
                    #     type(str(line_json["pub_id"])),
                    #     chembl_pubmed_ids_dict.keys()[1],
                    #     type(chembl_pubmed_ids_dict.keys()[1])
                    # ))
                    # exit()
                    # print(line_json["pub_id"])
                    if str(line_json["pub_id"]) in chembl_pubmed_ids_dict:
                        in_chembl += 1
                        logging.error('ok %s' % str(in_chembl))
                        #exit()
                        new_line.append(line)
                        if line[-1] == '\n':
                            # counter += 1
                            if len(new_line) > 1:
                                line_to_yield = ''.join(new_line)
                            else:
                                line_to_yield = line
                            new_line = []
                            # doc = json.loads(line)
                            if line_to_yield:
                                pub_id = line_to_yield.partition('"pub_id": "')[2].partition('"')[0]
                                if not pub_id:
                                    logging.error('no pubmedid parsed for line %s' % line)
                                else:
                                    # print index_, doc_type, pub_id
                                    _id = None
                                    if use_pub_id and pub_id:
                                        _id = pub_id
                                        yield {
                                            '_index': index_,
                                            '_type': doc_type,
                                            '_id': _id,
                                            '_source': line_to_yield
                                        }
                                    else:
                                        yield {
                                            '_index': index_,
                                            '_type': doc_type,
                                            '_source': line_to_yield
                                        }
                break
        except Exception as e:
            logging.exception('could not get file %s: %s' % (file_name, e))
            pass
        if counter == 3:
            logging.error(' file %s skipped', file_name)

        #logging.info('pubmed ids in chembl loaded: %s of %s' % (in_chembl, len(chembl_pubmed_ids)))


# def load_file(index_, doc_name, file_name):
#     return list(read_remote_file(index_, doc_name, file_name))
    # for line in read_remote_file(file_name, index_, doc_name):
    #     yield line


def get_file_names(path):
    #client = storage.Client(project='open-targets')
    client = storage.Client(project='siren-pubmed')
    #client = storage.Client()
    bucket = client.get_bucket('medline-json')

    for i in bucket.list_blobs(prefix='splitted/'):
        #if i.name.endswith(path):
        if fnmatch(i.name, ('splitted/'+path)):
            yield i.name


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('indices', nargs='+',
                        help='indices to load')
    parser.add_argument('-es', dest='es', action='append',
                        default=[],
                        help='elasticsearch urls')
    parser.add_argument('-username', default='demo', required=False)
    parser.add_argument('-password', default='enterprise123#', required=False)
    args = parser.parse_args()

    ES_AUTH = (args.username, args.password)

    p = Pool(4)
    valid_indices = list(set(args.indices) & set(index_config.keys()))
    logging.info('loading data for indices: ' + ', '.join(valid_indices))
    es = Elasticsearch(
        hosts=args.es,
        max_retry=10,
        retry_on_timeout=True,

        use_ssl=True,
        verify_certs=False,
        http_auth=ES_AUTH,
        connection_class=RequestsHttpConnection
    )
    for idx in tqdm(valid_indices,
                    desc='loading indexes',
                    unit='indexes'):
        index_data = index_config[idx]

        '''prepare es for loading'''
        tqdm.write('deleting %s %s' % (
            index_data['index'], es.indices.delete(index=index_data['index'], ignore=404, timeout='300s')))
        if index_data['mappings']:
            tqdm.write('creating %s %s' % (
                index_data['index'], es.indices.create(index=index_data['index'], ignore=400, timeout='30s',
                                                       body=json.load(open('es-mapping/' + index_data['mappings']))
                                                       )))
        else:
            tqdm.write('creating %s %s' % (
                index_data['index'], es.indices.create(index=index_data['index'], ignore=400, timeout='30s',
                                                       )))
        time.sleep(3)
        temp_index_settings = {
            "index": {
                "refresh_interval": "-1",
                "number_of_replicas": 0,
                "translog.durability": 'async',
            }
        }
        es.indices.put_settings(index=index_data['index'],
                                body=temp_index_settings)
        success, failed = 0, 0
        '''get data'''
        # func = partial(load_file, index_data['index'], index_data['doc_type'])

        with tqdm(
                desc='loading json for index %s' % index_data['index'],
                unit=' docs',
                unit_scale=True,
                total=30000000 if 'concept' not in index_data['index'] else 570000000) as pbar:
            file_names = list(get_file_names(path=index_data['path']))
            chunk_size = 10
            file_pbar = tqdm(file_names,
                                  desc='files processed',
                                  unit=' files',
                                  unit_scale=True)
            malformed = {}

            for file_name in file_pbar:
                loaded_rows = read_remote_file(index_data['index'], index_data['doc_type'], file_name, malformed, index_data['pub_id'])
                counter = 0

                # for loaded_rows in tqdm(p.imap_unordered(func, get_file_names(path=index_data['path'])),
                #                         desc='files processed',
                #                         unit=' files',
                #                       unit_scale=True):
                #     chunksize = 500
                #     # threads = 1
                threads = 4
                for batchiter in grouper(loaded_rows, threads * chunk_size):
                    counter = 0
                    try:
                        for ok, item in parallel_bulk(es,
                                                    # loaded_rows,
                                                    batchiter,
                                                      raise_on_error=False,
                                                      chunk_size=chunk_size,
                                                      thread_count= threads,
                                                      request_timeout=300
                                                      ):

                            if not ok:
                                tqdm.write('failed', ok, item)
                                failed += 1
                            else:
                                success += 1
                            counter += 1
                    except Exception as e:
                        logging.exception(e)
                        failed += chunk_size * threads
                    pbar.update(counter)
                if file_pbar.total%10==0:
                    es.indices.flush(index_data['index'])
                #

        malformed_all = 0
        for f in malformed:
            malformed_all += malformed[f]
        tqdm.write("uploaded %i success, %i failed, %i malformed" % (success, failed, malformed_all))

        restore_index_settings = {
            "index": {
                "refresh_interval": "1s",
                "number_of_replicas": 1,
                "translog.durability": 'request',
            }
        }
        es.indices.put_settings(index=index_data['index'],
                                body=restore_index_settings)

