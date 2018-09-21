from graph_tool.all import *;

def unpack_id(g):
    # unpack variables for O(1) access
    q2v = {}
    p2v = {}
    qq2e = {}

    for v in g.vertices():
        if g.vp.item[v]:   #items => Q
            q2v[g.vp.q[v]] = v
        else:                     #property => P
            p2v[g.vp.q[v]] = v

    for e in g.edges():
        qq2e[g.edge_index[e]] = e

    return (q2v, p2v, qq2e)


def unpack_labels(g):
    # unpack label and aliases
    Labels = {} 
    labels = {} #lowercased

    # unpack english wikipedia titles
    enwiki = {}

    for v in g.vertices():
        if g.vp.label[v]:
            if g.vp.label[v].lower() in labels:
                labels[g.vp.label[v].lower()].append(v)
            else:
                labels[g.vp.label[v].lower()] = [v]

            if g.vp.label[v] in Labels:
                Labels[g.vp.label[v]].append(v)
            else:
                Labels[g.vp.label[v]] = [v]            
                
                
        if g.vp.aliases[v]:
            for alias in g.vp.aliases[v]:
                if alias.lower() in labels:
                    labels[alias.lower()].append(v)                
                else:
                    labels[alias.lower()] = [v]

                if alias in Labels:
                    Labels[alias].append(v)
                else:
                    Labels[alias] = [v]
                    
        if g.vp.enwiki[v]:
            enwiki[g.vp.enwiki[v]] = v  

    return (labels, Labels, enwiki)

#get two steps down the Wikimedia project page to disable all meta articles
def deactivate_wmprojectpages(g, q2v, q = 14204246): # Wikimedia project page (Q14204246)
    if 'wmprojectpages' in g.vertex_properties:
        wmprojectpages = g.vertex_properties["wmprojectpages"]
    else:
        wmprojectpages = g.new_vertex_property("bool", val=True) #create property for filtering

        if q in q2v:
            wmpp = q2v[q]
        else:
            print("Wikimedia project page (Q14204246) not in graph. ABORT")
            return (False, g)

        # all instace of and subclass of Wikimedia project page
        wmpps = g.get_in_edges(wmpp)

        for e in wmpps:
#            print(">>>>>", g.vp.label[g.vertex(e[0])], "(Q", g.vp.q[g.vertex(e[0])] , ")")
            wmprojectpages[g.vertex(e[0])] = False
            sc = 0
            if g.vp.q[g.vertex(e[0])] not in (5460604,):
                for ee in g.get_in_edges(e[0]):
                    sc = sc + 1

                    if sc < 10:
                        print(">>", g.vp.label[g.vertex(ee[0])])
                    if sc == 10:
                        print(">> ...")
                        
                    wmprojectpages[g.vertex(ee[0])] = False
                    
#            print("Total:", sc + 1)

        g.vertex_properties["wmprojectpages"] = wmprojectpages
            
    return (True, GraphView(g, vfilt=wmprojectpages))


def deactivate_properties(g):
    return (True, GraphView(g, vfilt=g.vp.item))
