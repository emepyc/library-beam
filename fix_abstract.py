import json
#from codecs import open
import gzip
import codecs
import logging

## Read the pubmed file
fname = '/Users/miguelpignatelli/Downloads/pubmed18-01426-of-03935_small.json.gz'

zf = gzip.open(fname, 'rb')
reader = codecs.getreader("utf-8")

#file = open(fname, 'r', encoding='utf8')
#file = open(fname, 'r')

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


# for line in reader(zf):
for (rec, line_json) in get_next_record(reader(zf)):
    logging.error(rec)

    #print(line)
    # l = codecs.decode(line, 'utf-8')
    #lines = line.split("\n")
    # l = line.replace("\n", "")
    #l = ''.join(lines)
    # try:
    #     j = json.loads(l)
    #     if j['pub_id'] == '28114141':
    #         print(type(j['pub_id']))
    #         print(j['pub_id'])
    #         print('ok')
    #         print(j)
    #         exit()
    #     #print(j)
    # except Exception as e:
    #     print('**************************************************')
    #     print(e)
    #     print(line)
    #     exit()

