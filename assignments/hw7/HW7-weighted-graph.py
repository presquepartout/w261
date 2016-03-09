%%writefile MrSssp_hw70.py
from numpy import argmin, array, random
import re
import numpy as np
from mrjob.job import MRJob, MRStep
from itertools import chain
import mrjob
import sys

class MrSssp_hw70(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    SORT_VALUES = True # Need to do secondary sort

    def mapper(self, nodeId, data):
        # data format:
        # Json object:
        # [ {adj-list dict}, [path from src], cost from src, status ]
        
        #print >> sys.stderr, "input data:", data
        adjList, pathFromSrc, costFromSrc, status = data
        
        if status == "Q":
            # The node is in Queued mode.  It's a frontier node.
            # Need to process its neighbours
            for neighbor, weight in adjList.items():
                neighbor = int(neighbor)
                neighborPathFromSrc = pathFromSrc + [neighbor]
                neighborCostFromSrc = costFromSrc + weight
                
                # Put 0 in the front for secondary sorting purpose
                yield neighbor, (0, None, neighborPathFromSrc, neighborCostFromSrc, "Q")
        
            # Lastly, change its own status to visited
            status = "V"

        # Put 0 in the front for secondary sorting purpose
        yield nodeId, (1, adjList, pathFromSrc, costFromSrc, status)
        
    def reducer(self, key, values):
        minCostFromSrc = sys.maxint
        emitted = False
        
        for data in values:
            assert not emitted, "Should not see more records after we've emitted result"
            
            sortkey, adjList, pathFromSrc, costFromSrc, status = data
            
            if adjList is None:
                assert status == 'Q', "status must be Q for record emitted by neighbor"
                
                # It is a record emitted from a neighbor
                if costFromSrc < minCostFromSrc:
                    minCostFromSrc = costFromSrc
                    minPathFromSrc = pathFromSrc                    
            else:
                
                if minCostFromSrc < costFromSrc:
                    # Its 'cost from Src' becomes smaller.  Put it to the frontier.
                    status = "Q"
                    costFromSrc = minCostFromSrc
                    pathFromSrc = minPathFromSrc            
                
                yield key, [adjList, pathFromSrc, costFromSrc, status] 

                emitted = True
                                                  
if __name__ == '__main__':
    MrSssp_hw70.run()
