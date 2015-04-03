#!/usr/bin/python
user_list = []
user_file = "user_list.txt"
with open(user_file) as users:
	for line in users:
		array = line.split("\t")
		if len(array)==2:
			user_list.append(array[0])

with open('graph_output.txt') as conf_file:
	for line in conf_file:	
		count = 0
		array = line.split(' ')
		for item in array[1:]:
			if item in user_list:
				count = count + 1
				#print item
		print '%s\t%s\t%s' %(array[0], len(array), count)

