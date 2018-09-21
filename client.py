import zerorpc

# connect to wikidata-server
wd = zerorpc.Client(timeout=600)
#wd.connect("ipc:///tmp/wikidata")
wd.connect("tcp://localhost:5555")


# helper to compile python code to execute on wikidata-server
def query(queryf, args):
    compile(queryf,'','eval') #only to check if it compiles before transmitting
    return wd.queryf(queryf, args)


# run code on server
print("# of vertices ", query('universe.num_vertices()', 1))

# run code on server
print("# of edges ", query('universe.num_edges()', 1))


# inspect all directly through RPC available functions
print("wikidata-server functions: ", wd._zerorpc_inspect())
