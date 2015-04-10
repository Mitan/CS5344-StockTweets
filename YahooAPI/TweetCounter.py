from itertools import groupby
from datetime import datetime
import math
import os

all_companies = {}


def check_if_not_weekend(day_string):
    day = datetime.strptime(day_string, '%Y-%m-%d').weekday()
    if day == 5 or day == 6:
        return False
    return True


# get unique user counts and weighted user counts
def get_users_statistics(day_group_list):
    unique_user_keys = []
    unique_user_weights = []
    for key, user_group in groupby(day_group_list, lambda x: (x.split()[-2])):
        unique_user_keys.append(key)
        one_user_messages = list(user_group)
        user_followers = (one_user_messages[0]).split()[-1]
        if int(user_followers) <= 100:
            user_weight = len(one_user_messages)
        else:
            user_weight = len(one_user_messages) * math.log(int(user_followers), 100)
        unique_user_weights.append(user_weight)
    ans = [len(unique_user_keys), sum(unique_user_weights)]
    return ans


def normalization_volume_weekly(target_list):
    target_list_copy = list(target_list)
    l = len(target_list_copy)
    number_of_weeks = l / 5
    for i in range(number_of_weeks):
        local_max = max(target_list_copy[i * 5: (i + 1) * 5])
        target_list_copy[i * 5: (i + 1) * 5] = map(lambda y: y / float(local_max), target_list_copy[i * 5: (i + 1) * 5])

    # if we have not integer number of weeks, need to normalize the tail
    last_few_days = l % 5
    if last_few_days != 0:
        local_max =  max(target_list_copy[-last_few_days :])
        target_list_copy[-last_few_days : ] = map(lambda y: y / float(local_max), target_list_copy[-last_few_days : ])

    return target_list_copy


def normalize_and_output(dates, data, output_file):
    normalized_data = normalization_volume_weekly(data)
    dates_and_data = zip(dates, normalized_data)
    dates_and_data_for_output = map(lambda v: v[0] + '\t' + str(v[1]) + '\n', dates_and_data)
    output_file = open(output_file, 'w')
    output_file.writelines(dates_and_data_for_output)
    output_file.close()


def process_one_file(input_filename):
    data_by_day = {}
    # read file input
    with open("./raw_twitter_input/" + input_filename, 'r') as input_file:
        lines = input_file.readlines()

    # group tweets by date
    for key, day_group in groupby(lines, lambda x: (x.split()[1])):
        # store count per day
        day_group_list = list(day_group)
        users, weighted_users = get_users_statistics(day_group_list)
        data_by_day[key] = [len(day_group_list), users, weighted_users]

    data_by_day = data_by_day.items()
    # don't have stock data for 03-07
    items_removed_weekends = [v for v in data_by_day if check_if_not_weekend(v[0]) and v != "2015-03-07"]
    sorted_result = sorted(items_removed_weekends, key=lambda tup: tup[0])

    dates = [v[0] for v in sorted_result]
    sorted_tweet_counts = [v[1][0] for v in sorted_result]
    sorted_tweet_users = [v[1][1] for v in sorted_result]
    sorted_tweet_weight_users = [v[1][2] for v in sorted_result]

    normalize_and_output(dates, sorted_tweet_counts, "./twitter_data/counts/counts_" + input_filename)
    normalize_and_output(dates, sorted_tweet_users, "./twitter_data/users/users_" + input_filename)
    normalize_and_output(dates, sorted_tweet_weight_users,
                         "./twitter_data/weighted_users/weighted_users_" + input_filename)


if __name__ == "__main__":
    files = [f for f in os.listdir('./raw_twitter_input')]
    for f in files:
        if f.endswith(".txt"):
            process_one_file(f)
