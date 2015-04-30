#!/usr/bin/python
import subprocess
import sys
initial_file_name = 'initial_pagerank.txt'
def main(argv):
	initial_file = open(initial_file_name,'w')
	output = subprocess.Popen(["wc","-l",argv[0]], stdout=subprocess.PIPE).communicate()[0]
	output = output.split(' ')
	print output[0]
	num_line = int(output[0])
	print 'num of line %s' %(num_line)
	if len(argv) != 1:
		return
	graph_file = argv[0]
	with open(graph_file) as conf_file:
		for line in conf_file:	
			result_string = ''
			node_array = line.split(' ')
			result_string += '%s\t%s\n'%(node_array[0].rstrip().lstrip(),1.0)
			initial_file.write(result_string)
	initial_file.close()
if __name__ == "__main__":
   main(sys.argv[1:])
