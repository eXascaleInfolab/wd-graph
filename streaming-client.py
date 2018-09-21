import zerorpc

# connect to wikidata-server
wd = zerorpc.Client(timeout=600)
wd.connect("ipc:///tmp/wikidata")

# stream answers
for v in wd.vertices():
    print (wd.claims(v))
