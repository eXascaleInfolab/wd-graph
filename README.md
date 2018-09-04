# wd-graph-tool


## Server
You can start the server with 
```
    usage: wikidata-server.py [-h] [--endpoint ENDPOINT] [--graph GRAPH]
```

## Client

### Connect to the Server

```
import zerorpc

wd = zerorpc.Client(heartbeat=20, timeout=6000)
wd.connect("ipc:///tmp/wikidata")

print(wd._zerorpc_inspect()) //check the available functions
```

### Run arbitrary function on the graph.

```
def query(queryf, args):
    compile(queryf,'','eval')
    return wd.queryf(queryf, args)

query('len(list(universe.edges()))', 1)
```
