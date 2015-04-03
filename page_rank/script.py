from collections import OrderedDict
inputs=OrderedDict()
updated_rank=OrderedDict()
beta = 0.8
# limit on matrix calculations
limit=100000

class inode:
     def __init__(self, id, numFollowing, followers):
         self.id = id
         self.numFollowing = numFollowing
         self.followers = followers
         self.page_rank = 0.0

with open('graph.txt', 'r') as f:
	for i in f:
         l = i.rstrip().split(" ")
         inputs[l[0]]=inode(l[0], int(l[1]), l[2:])

for i,j in inputs.iteritems():
    updated_rank[i] = 1.0/len(inputs)

def calculate_rank(node, inputs):
    rank = 0.0
    for i in node.followers:
        follower_node = inputs[i]
        rank = rank + ((1.0/follower_node.numFollowing) * follower_node.page_rank)
    updated_rank[node.id] = (beta * rank) + ((1-beta)/len(inputs))

for j in inputs.itervalues():
    j.page_rank = updated_rank[j.id]

for x in xrange(limit):
    # matrix calculation
    for i,j in inputs.iteritems():
        calculate_rank(j,inputs)
    # check if updated rank is same as old one
    alleq = True
    for i,j in updated_rank.iteritems():
        if inputs[i].page_rank != j:
          alleq = False
          break
    if alleq:
        print "breaking at ",x
        break
    # update ranks
    for j in inputs.itervalues():
        j.page_rank = updated_rank[j.id]

for j in inputs.itervalues():
    print j.id, j.page_rank