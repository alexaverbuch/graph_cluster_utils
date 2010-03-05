import random

class EvolvingSetRecord:
    def __init__(self,evoprocess):
        self.particle      =  evoprocess.particle
        self.volume        =  evoprocess.volume  
        self.cutsize       =  evoprocess.cutsize 
        self.conductance   =  evoprocess.conductance 
        self.totalcost     =  evoprocess.totalcost    
        
    def write(self,f):
        f.write("current particle location: %s\n"%self.particle)
        f.write("current set volume: %s\n"%self.volume)
        f.write("current set cutsize: %s\n"%self.cutsize)
        f.write("current set conductance: %s\n"%self.conductance)
        f.write("process totalcost: %s\n"%self.totalcost)

class EvolvingSetFullRecord:
    def __init__(self,evoprocess):
        self.nodes = [x for x in self.evoset]
        self.boundary = [x for x in self.evoset.boundary_iter()]
        self.particle      =  evoprocess.particle
        self.volume        =  evoprocess.volume  
        self.cutsize       =  evoprocess.cutsize 
        self.conductance   =  evoprocess.conductance 
        self.totalcost     =  evoprocess.totalcost    

class EvolvingSetProcess:
    def __init__(self,graph,startnode):
        self.graph     = graph
        self.evoset    = SetWithBoundary(graph,[startnode])
        self.particle  = startnode
        
        self.cutsize = self.evoset.cutsize
        self.volume = self.evoset.volume
        self.conductance = self.compute_conductance()
   
        self.totalcost = 0
        self.steps = 0

    def compute_conductance(self):
        totalvol = self.graph.numarcs
        vol = self.evoset.volume
        cut = self.evoset.cutsize
        return cut/float(min(vol,totalvol-vol))

    def get_thresh(self,v):
        degv = self.graph.degree[v]
        es = self.evoset.getedgestoset(v)
        inset = 1 if v in self.evoset else 0
        return .5*(es/float(degv) + inset)
    
    def step(self):
        # move the particle to a random neighbor of its current location
        self.particle = random.choice(self.graph.nbrs[self.particle])   
        
        # the new threshold is the score for the new location 
        thresh = self.get_thresh(self.particle)                        

        swapnodes = []
        # if threshold is greater than half, the set grows
        # add new nodes that are above the threshold 
        if thresh <= .5:
            swapnodes = [x for x in self.evoset.boundary_iter() if self.get_thresh(x) >= thresh and x not in self.evoset]
            for x in swapnodes:
                self.evoset.addnode(x)
        
        # if threshold is greater than half, the set shrinks
        # remove nodes below the threshold 
        if thresh > .5:
            swapnodes = [x for x in self.evoset.boundary_iter() if self.get_thresh(x) < thresh and x in self.evoset]
            for x in swapnodes:
                self.evoset.removenode(x)

        self.cutsize = self.evoset.cutsize
        self.volume = self.evoset.volume
        self.conductance = self.compute_conductance()
        self.totalcost += sum(self.graph.degree[x] for x in swapnodes)
        self.steps += 1

class SetWithBoundary:
    """Data structure for holding a set S
        and counting the number of edges e(x,S) to the set for each boundary node x.
      
      Invariants:
      nodeset    is the set of nodes in the current evolving set.
      boundary   is a dict that maps x -> e(x,S) for each boundary node.
                  a boundary node is a node where some but not all of its edges go to S,
                  in other words x is a boundary node if e(x,S) > 0 and e(x,S) < d(x).
    """
    def __init__(self,graph,startset):
        self.graph      = graph
        self.nodeset    = set()
        self.edgestoset = dict()
        
        self.volume   = 0
        self.cutsize  = 0

        for x in startset:
            self.addnode(x)
    
    def __contains__(self,x): 
        return x in self.nodeset
    
    def __iter__(self): 
        return self.nodeset.__iter__()

    def in_boundary(self,x):
        return x in self.edgestoset
    
    def should_be_in_boundary(self,x):
        inboundary = False
        if ( (x in self.nodeset) and (self.getedgestoset(x) < self.graph.degree[x]) ):
            inboundary = True
        if ( (x not in self.nodeset) and (self.getedgestoset(x) > 0)):
            inboundary = True
        return inboundary 

    def boundary_iter(self):
        return self.edgestoset.__iter__()
 
    def getedgestoset(self,x): 
        if x in self.edgestoset:
            return self.edgestoset[x]
        else:
            if x in self.nodeset:
                return self.graph.degree[x]
            else:
                return 0

    def addnode(self,x):
        if x not in self.nodeset:
            nbrs = self.graph.nbrs
            degree = self.graph.degree
            
            #add to nodeset
            self.nodeset.add(x)  
            self.volume += degree[x]
            
            #update neighbors 
            for y in nbrs[x]:            
                self.edgestoset[y] = self.getedgestoset(y)+1
                if not self.should_be_in_boundary(y):
                    self.edgestoset.pop(y)
                if y in self.nodeset:       
                    self.cutsize -= 1
                else:
                    self.cutsize += 1
            
            #compute own boundary
            self.edgestoset[x] = sum(1 for y in nbrs[x] if y in self.nodeset)            
            if not self.should_be_in_boundary(x):
                self.edgestoset.pop(x)

    def removenode(self,x):
        if x in self.nodeset:
            nbrs = self.graph.nbrs
            degree = self.graph.degree

            #remove from nodeset     
            self.nodeset.remove(x)    
            self.volume -= degree[x]
            
            #maybe remove from boundary
            if not self.should_be_in_boundary(x):
                self.edgestoset.pop(x)
 
            #update neighbors 
            for y in nbrs[x]:                                
               self.edgestoset[y] = self.getedgestoset(y)-1
               if not self.should_be_in_boundary(y):
                   self.edgestoset.pop(y)
               if y in self.nodeset:         
                   self.cutsize += 1
               else:
                   self.cutsize -= 1

    def test(self):
        curnodeset = set(self.nodeset)
        failed = False
        for x in range(self.graph.numnodes):
            true_edgestoset = sum(1 for y in self.graph.nbrs[x] if y in curnodeset)
            reported_edgestoset = self.getedgestoset(x)
            if true_edgestoset != reported_edgestoset:
                print "%s: (true,reported) = (%s,%s)"%(x,true_edgestoset,reported_edgestoset)
                print "nbrs: %s"%self.graph.nbrs[x]
                failed = True
        if failed:
            print 'failed test: edgestoset is not correct for all nodes in the graph'
        else:
            print 'passed test: edgestoset is correct for all nodes in the graph'

        boundaryfail = False
        for x in self.boundary_iter():
            if not self.should_be_in_boundary(x):
                boundaryfail = True
                print "%s : %s"%(x,self.graph.nbrs[x])
                print "degree: %s"%self.graph.degree[x]
                print "getedgestoset: %s"%self.getedgestoset(x)
                print "inset: %s"%(x in self)
                print "edgestoset: %s"%self.edgestoset[x]
        if boundaryfail:
            print "boundary test failed: boundary was too big"
        else:
            print "boundary test passed"
