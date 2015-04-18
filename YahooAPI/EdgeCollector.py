import os
from TweetCounter import check_if_not_weekend, normalization_volume

__author__ = 'Dmitrii'

files = [f for f in os.listdir('./edges_raw/')]
for f in files:
    if os.path.isfile('./edges_raw/' + f):
        company_name = f[6:]
        with open('./edges_raw/' + f) as the_file:
            lines = the_file.readlines()
        filtered_lines = [v.split() for v in lines if check_if_not_weekend(v.split()[0])]
        dates = [v[0] for v in filtered_lines]
        values = [float(v[1]) for v in filtered_lines]
        values = normalization_volume(values, company_name)
        normalized_data = zip(dates,values)
        normalized_data_for_output = [v[0] + '\t' + str(v[1]) + '\n' for v in normalized_data]
        out_file = open('./edges_raw/normalized_edges/' + company_name + '.txt','w')
        out_file.writelines(normalized_data_for_output)
        out_file.close()