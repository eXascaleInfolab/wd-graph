# wd-graph-tool

## Transform
Transform a Wikidata JSON Dump to a GraphTool Binary Graph.

```
usage: create.py [-h] [--dump DUMP]

optional arguments:
  -h, --help            show this help message and exit
  --dump DUMP, -d DUMP  the wikidata dump to load (wikidata-*-all.json.bz2)
```

This process does not run in parallel and takes up to multiple days.
The whole graph will be build in memory. (~500GB)

## Server

Start Wikidata Graph Server.

```
usage: server.py [-h] [--endpoint ENDPOINT] [--graph GRAPH]

optional arguments:
  -h, --help            show this help message and exit
  --endpoint ENDPOINT, -e ENDPOINT
                        the zmq endpoint to listen to (default
                        ipc:///tmp/wikidata)
  --graph GRAPH, -g GRAPH
                        the graph to load
```

The whole graph will be loaded into memory. (~500GB)

## Client

### Connect to the Server

```
import zerorpc

wd = zerorpc.Client(heartbeat=20, timeout=6000)
wd.connect("ipc:///tmp/wikidata") //connect to the provided endpoint

print(wd._zerorpc_inspect()) //check the available functions
```

### Run arbitrary function on the graph.

```
def query(queryf, args):
    compile(queryf,'','eval')
    return wd.queryf(queryf, args)

query('len(list(universe.edges()))', 1)
```
