import zerorpc
from gevent import monkey; monkey.patch_all()
from graph_tool.all import *;
import argparse
from wdgraph.utils import *;

parser = argparse.ArgumentParser(description='Start Wikidata Graph Server.')
parser.add_argument('--endpoint', '-e', default="ipc:///tmp/wikidata", help='the zmq endpoint to listen to (default ipc:///tmp/wikidata)')
parser.add_argument('--graph', '-g', default="datasets/SAMPLE.universe.gt.bz2", help='the graph to load')
args = parser.parse_args()

universe = 0

class wd(object):
    def __init__(self, filename):
        self.load(filename)
        self.unpack_id()

        self.deactivate_wmprojectpages()
        self.deactivate_properties()

        self.unpack_labels()
        self.build_pagerank()
    
    def load(self, filename):
        global universe
        self.filename = filename
        if not type(universe) == Graph:
            self.full_universe = universe = load_graph(filename)
            print("✔ loaded ", filename)
        return True

    def unpack_id(self):
        if not hasattr(self, 'q2v'):
            self.q2v, self.p2v, self.qq2e = unpack_id(universe)

        print("✔ unpacked ids")
        return True

    def unpack_labels(self):
        if not hasattr(self, 'labels'):
            self.labels, self.Labels, self.enwiki = unpack_labels(universe)

        print("✔ unpacked labels")
        return True

    def build_pagerank(self, damping=0.85):
        universe.vertex_properties["pr"] = pagerank(universe, damping=damping)
        print("✔ build pagerank")
        return True

    def deactivate_wmprojectpages(self): 
        global universe
        success, universe = deactivate_wmprojectpages(universe, self.q2v)

        if success:
            print("✔ deactivated  WikiMedia Project Pages")
        return success

    def deactivate_properties(self): 
        global universe
        universe = deactivate_properties(universe)

        print("✔ deactivated Property Nodes (e.g. P31)")


    def get_filename(self):
        return self.filename

    #provides query access
    def queryf(self, func, args):
        return eval(func)

    #vertices streaming
    @zerorpc.stream
    def vertices(self):
        for v in universe.vertices():
            yield int(v)
 
    #edges streamin
    @zerorpc.stream
    def edges(self):
        for e in universe.edges():
            yield int(e)


    def get_out_edges(self, vertex):
        return universe.get_out_edges(vertex).tolist()

    def prop(self, name, vertex):
        if type(vertex) == 'list':
            vertex = tuple(vertex)
        return universe.properties[(name[0],name[1])][vertex]

    def dict(self, name, key):
        attr = getattr(self,name)[key]
        if type(attr) == 'list':
            return list(map(lambda x: int(x), getattr(self,name)[key]))
        else:
            return int(attr)

    def claims(self, vertex):
        p2v = self.p2v
        qq2e = self.qq2e

        clean = []
        v = universe.vertex_index[vertex]
        # attributes
#        if universe.vp.attributes[v]:
#            for a in universe.vp.attributes[v]:
#                if 'datavalue' in a and a['datatype'] in ('string'):
#                    if int(a['property'][1:]) in p2v:
#                        clean.append((universe.vp.label[p2v[int(a['property'][1:])]], a['datavalue']['value']))
            
        # edges
        edges = universe.get_out_edges(v)
        for e in edges:
            if e[2] in qq2e:
                p = universe.ep.p[qq2e[e[2]]]
                if p in p2v:
                    # TODO incorporate aliases
                    key = universe.vp.label[p2v[p]]
                    value = universe.vp.label[e[1]]
                    if key and value:
                        clean.append((key, value))
                else:
                    print("Missing property P"+str(p))

        return clean

    def claims_pr(self, vertex):
        p2v = self.p2v
        qq2e = self.qq2e

        clean = []
        v = universe.vertex_index[vertex]
            
        # edges
        edges = universe.get_out_edges(v)
        for e in edges:
            if e[2] in qq2e:
                p = universe.ep.p[qq2e[e[2]]]
                if p in p2v:
                    key = universe.vp.label[p2v[p]]
                    value = universe.vp.label[e[1]]
                    pr = 0
                    try:
                        pr = universe.vp.pr[e[1]]
                        if key and value:
                            clean.append((key, value, pr))
                    except KeyError:
                        pass
                else:
                    print("Missing property P"+str(p))

        return clean


    def claims_v(self, vertex):
        qq2e = self.qq2e
        p2v = self.p2v

        clean = []
        v = universe.vertex_index[vertex]
            
        # edges
        edges = universe.get_out_edges(v)
        for e in edges:
            if e[2] in qq2e:
                p = universe.ep.p[qq2e[e[2]]]
                if p in p2v:
                    clean.append((int(p2v[p]), int(e[1])))
                else:
                    print("Missing property P"+str(p))

        return clean


    def shortest_paths(self, source, target):
        universe.set_directed(False)
        paths = all_shortest_paths(universe, universe.vertex_index[source], universe.vertex_index[target])
        universe.set_directed(True)

        result = []
        for path in paths:
#            result.append(list(map(lambda x: universe.vp.label[x], path)))
            result.append(list(path))

        return result


s = zerorpc.Server(wd(args.graph), heartbeat=None)
s.debug = True

s.bind(args.endpoint)
print("Listening at: ", args.endpoint)
s.run()
