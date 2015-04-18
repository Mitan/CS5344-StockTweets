__author__ = 'Dmitrii'

import os

folders_list = ['./stock_data/companies/normalized_companies/', "./stock_data/currencies/normalized_currencies/",
                "./twitter_data/average_pr/", "./twitter_data/full_pr/", "./twitter_data/counts",
                "./twitter_data/users/",
                "./twitter_data/weighted_users/", "./twitter_data/weighted_pr_users/"]
for folder in folders_list:
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        if os.path.isfile(file_path):
            os.unlink(file_path)