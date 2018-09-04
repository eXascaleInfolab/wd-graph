import zerorpc

wd = zerorpc.Client(timeout=600)
wd.connect("ipc:///tmp/wikidata_small")

wd.build_pagerank()

for v in wd.vertices():
    print(wd.prop(('v','pr'), v))
