from collections import OrderedDict
p = OrderedDict()
with open("appl.txt") as f:
	for i in f:
		l=i.rstrip().split()
		k=l[0]
		v=l[1]
		if p.has_key(k):
			p[k] = int(p[k]) + int(v)
		else:
			p[k] = int(v)

for h,a in p.iteritems():
	print h,a
			
