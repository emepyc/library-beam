import argparse
import elasticsearch as es
import logging


def delete(docid_list, host, port, index):
    elastic = es.Elasticsearch([{'host': host, 'port': port}])
    body = {
        "query": {
            "terms": {
                "_id": docid_list
            }
        }
    }
    logging.info( elastic.delete_by_query(index=index, body=body) )

def docidbatch(f, size=100):
    batch = []
    for line in open(f):
        batch.append(line.strip())
        if len(batch) == 100:
            yield batch
            batch = []

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Delete docs from pubmed')
    parser.add_argument('index',  help='index to delete from')
    parser.add_argument("-m", "--host", dest="host", help="Host elastic is running on")
    parser.add_argument("-p", "--port", dest="port", help="Port elastic is running on")
    parser.add_argument('-username', required=False)
    parser.add_argument('-password',  required=False)
    parser.add_argument('-deletefile', dest='deletefile', help='file containing doc ids to delete(not gzipped')
    parser.add_argument('-logfile', dest='logfile', help='file containing doc ids to delete(not gzipped')

    args = parser.parse_args()
    logging.basicConfig(filename=args.logfile, level=logging.DEBUG)



    count = 0
    for batch in docidbatch(args.deletefile):
        count += 1
        print ' batch ', count
        delete(batch, args.host, args.port, args.index)


