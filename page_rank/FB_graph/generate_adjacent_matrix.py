#!/usr/bin/python
import sys
isolated_users_file = 'zero_link.txt'
filtered_users_file = 'FB_filtered_graph.txt'
def main(argv):
	isolated_f = open(isolated_users_file,'w')
	filtered_f = open(filtered_users_file ,'w')

	if len(argv) != 2:
		return
	user_dict = {}
	user_file = argv[0]
	graph_file = argv[1]
	with open(user_file) as users:
		for line in users:
			array = line.split("\t")
			if len(array)==2:
				user_dict[array[0]]=[]

	with open(graph_file) as conf_file:
		for line in conf_file:	
			count = 0
			node_array = line.split(' ')
			for item in node_array[1:]:
				if item in user_dict:
					user_dict[node_array[0]].append(item)
					count = count + 1
					#print item
			if count == 0:
				node_id = node_array[0].rstrip().lstrip()
				isolated_f.write(node_id+'\n')
			else:
				result_string = ''
				result_string += node_array[0]
				result_string += ' %s'%(count)
				for adjacent_node in  user_dict[node_array[0]]:
					result_string += ' %s'%(adjacent_node)
				result_string+='\n'
				filtered_f.write(result_string)
	filtered_f.close()
	isolated_f.close()
		

if __name__ == "__main__":
   main(sys.argv[1:])
