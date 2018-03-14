import json

path = '/Users/miguelpignatelli/Downloads/pubmed18-00000-of-03935_taggedtext.json'

with open(path) as f:
    for i, line in enumerate(f):
        j = json.loads(line)
        pid = str(j['pub_id'])
        print(pid)

    f.close()
