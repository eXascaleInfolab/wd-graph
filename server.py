import zerorpc
from gevent import monkey; monkey.patch_all()
from graph_tool.all import *;
import argparse

parser = argparse.ArgumentParser(description='Start Wikidata Graph Server.')
parser.add_argument('--endpoint', '-e', default="ipc:///tmp/wikidata", help='the zmq endpoint to listen to (default ipc:///tmp/wikidata)')
parser.add_argument('--graph', '-g', default="datasets/wikidata-20170424-all.json.bz2.universe.gt.bz2", help='the graph to load')
args = parser.parse_args()

universe = 0

class wd(object):
    def __init__(self, filename):
        self.load(filename)
        self.unpack_id()
        self.unpack_labels()
        self.deactivate_wmprojectpages()
        self.build_pagerank()
    
    def load(self, filename):
        global universe
        self.filename = filename
        if not type(universe) == Graph:
            universe = load_graph(filename)
            print("✔ loaded ", filename)
        return True

    def unpack_id(self):
        if not hasattr(self, 'q2v'):
            self.q2v = {}
            self.p2v = {}
            for v in universe.vertices():
                if universe.vp.item[v]: #items => Q
                    self.q2v[universe.vp.q[v]] = v
                else: #property => P
                    self.p2v[universe.vp.q[v]] = v

            self.qq2e = {}
            for e in universe.edges():
                self.qq2e[universe.edge_index[e]] = e

        print("✔ unpacked ids")
        return True

    def unpack_labels(self):
        if not hasattr(self, 'labels'):
            # unpack label and aliases
            self.labels = {}
            # unpack english wikipedia titles
            self.enwiki = {}

            for v in universe.vertices():
                if universe.vp.label[v]:
                    if universe.vp.label[v] in self.labels:
                        self.labels[universe.vp.label[v]].append(v)
                    else:
                        self.labels[universe.vp.label[v]] = [v]

                if universe.vp.aliases[v]:
                    for alias in universe.vp.aliases[v]:
                        if alias in self.labels:
                            self.labels[alias].append(v)
                        else:
                            self.labels[alias] = [v]
                            
                if universe.vp.enwiki[v]:
                    self.enwiki[universe.vp.enwiki[v]] = v
        print("✔ unpacked labels")
        return True

    def build_pagerank(self, damping=0.85):
        universe.vertex_properties["pr"] = pagerank(universe, damping=damping)
        print("✔ build pagerank")
        return True

    def get_filename:
        return self.filename

    @zerorpc.stream
    def vertices(self):
        for v in universe.vertices():
            yield int(v)

    @zerorpc.stream
    def edges(self):
        for e in universe.edges():
            yield int(e)


    def get_out_edges(self, v):
        return universe.get_out_edges(v).tolist()

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

    def queryf(self, func, args):
        return eval(func)

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

    #get two steps down the Wikimedia project page to disable all meta articles
    def deactivate_wmprojectpages(self, q = 14204246): # Wikimedia project page (Q14204246)
        q2v = self.q2v
        deactivated = universe.new_vertex_property("bool", val=False) #create property for filtering
        wmpp = q2v[q]
        
        # all instace of and subclass of Wikimedia project page
        wmpps = universe.get_in_edges(wmpp)
        
        for e in wmpps:
#            print(">>>>>", universe.vp.label[universe.vertex(e[0])], "(Q", universe.vp.q[universe.vertex(e[0])] , ")")
            deactivated[universe.vertex(e[0])] = True
            sc = 0
            if universe.vp.q[universe.vertex(e[0])] not in (5460604, 30849):
                for ee in universe.get_in_edges(e[0]):
                    if sc < 10:
                        sc = sc + 1
#                        print(">>", universe.vp.label[universe.vertex(ee[0])])
                    deactivated[universe.vertex(ee[0])] = True
#            print("Total:", sc + 1)
        universe.set_vertex_filter(deactivated, inverted=True)
        print("✔ deactivated  WM Project Pages")

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
