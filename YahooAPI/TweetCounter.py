from itertools import groupby
import datetime as dt

import math
import os

all_companies = {}


def check_if_not_weekend(day_string):
    try:
        day = dt.datetime.strptime(day_string, '%Y-%m-%d').weekday()
    # catch bad format tweets
    except:
        return False
    if day == 5 or day == 6:
        return False
    return True


# get unique user counts and weighted user counts for one day
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

# get pagerank statistics for one day
def get_page_statistics(day_group_list, page_rank_values):
    unique_user_weights = []
    unique_user_ranks = []
    #key
    for key, user_group in groupby(day_group_list, lambda x: (x.split()[-2])):
        one_user_messages = list(user_group)
        user_weight = len(one_user_messages) * page_rank_values[key]
        user_rank = page_rank_values[key]
        unique_user_weights.append(user_weight)
        unique_user_ranks.append(user_rank)
    # return score for given day with weight of user pr and average pr a day
    ans = [sum(unique_user_weights), sum(unique_user_ranks) / float(len(unique_user_ranks))]
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


def normalize_and_output(dates, data, output_file, given_period, exlude_period):
    normalized_data = normalization_volume_weekly(data)
    dates_and_data = zip(dates, normalized_data)
    filtered_dates_and_data = [x for x in dates_and_data if x[0] in given_period and (not x[0] in exlude_period)]
    dates_and_data_for_output = map(lambda v: v[0] + '\t' + str(v[1]) + '\n', filtered_dates_and_data)
    output_file = open(output_file, 'w')
    output_file.writelines(dates_and_data_for_output)
    output_file.close()

# process all but output just given list
def process_one_file(input_filename, period, exclude):
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
    items_removed_weekends = [v for v in data_by_day if check_if_not_weekend(v[0])]
    sorted_result = sorted(items_removed_weekends, key=lambda tup: tup[0])

    dates = [v[0] for v in sorted_result]
    sorted_tweet_counts = [v[1][0] for v in sorted_result]
    sorted_tweet_users = [v[1][1] for v in sorted_result]
    sorted_tweet_weight_users = [v[1][2] for v in sorted_result]


    normalize_and_output(dates, sorted_tweet_counts, "./twitter_data/counts/counts_" + input_filename, period, exclude)
    normalize_and_output(dates, sorted_tweet_users, "./twitter_data/users/users_" + input_filename, period, exclude)
    normalize_and_output(dates, sorted_tweet_weight_users,
                         "./twitter_data/weighted_users/weighted_users_" + input_filename, period, exclude)

# process all but output just given list
def process_one_file_for_pagerank(input_filename, period, exclude, rank_path):
    data_by_day = {}
    # read file input
    with open("./raw_twitter_input/" + input_filename, 'r') as input_file:
        lines = input_file.readlines()

    #get rank values
    with open(rank_path, 'r') as input_rank_file:
        rank_lines = input_rank_file.readlines()

    rank_values = {}
    for line in rank_lines:
        user_id, rank = line.split()
        rank_values[user_id] = float(rank)

    # group tweets by date
    for key, day_group in groupby(lines, lambda x: (x.split()[1])):
        # store count per day
        if key not in period:
            continue
        day_group_list = list(day_group)
        data_by_day[key] = get_page_statistics(day_group_list, rank_values)

    data_by_day = data_by_day.items()
    # don't have stock data for 03-07
    items_removed_weekends = [v for v in data_by_day if check_if_not_weekend(v[0])]
    sorted_result = sorted(items_removed_weekends, key=lambda tup: tup[0])

    dates = [v[0] for v in sorted_result]
    sorted_weight_pr = [v[1][0] for v in sorted_result]
    sorted_average_pr = [v[1][1] for v in sorted_result]

    normalize_and_output(dates, sorted_weight_pr, "./twitter_data/weighted_pr_users/weighted_pr_users_" + input_filename, period, exclude)
    normalize_and_output(dates, sorted_average_pr, "./twitter_data/average_pr/average_pr_" + input_filename, period, exclude)


def calculate_timerange(start, end):
    start = dt.datetime.strptime(start, '%Y-%m-%d')
    end = dt.datetime.strptime(end, '%Y-%m-%d')
    day_count = (end - start).days + 1
    r = [single_date for single_date in (start + dt.timedelta(n) for n in range(day_count))]
    r = map(lambda x: x.strftime("%Y-%m-%d"), r)
    return r

if __name__ == "__main__":
    start_date = '2015-03-01'
    end_date = '2015-04-11'
    period_range = calculate_timerange(start_date, end_date)

    currencies = ["USDJPY", "EURUSD", "EURGBP"]


    files = [f for f in os.listdir('./raw_twitter_input')]
    for f in files:
        if f.endswith(".txt"):
            #make a list of dates, for which we don't have stock data. it is different for currencies and companies
            name = f[:-4]
            exclude_range = [] if name in currencies else ['2015-04-03']
            process_one_file(f, period_range, exclude_range)

    # process pr
    start_date_pr = '2015-03-01'
    end_date_pr = '2015-03-28'
    period_range_pr = calculate_timerange(start_date_pr, end_date_pr)


    pr_symbols = ["AMZN", "BABA", "EURUSD", "FB", "YHOO", "QQQ", "SPY", "USDJPY"]
    for symbol in pr_symbols:
        exclude_range = [] if symbol in currencies else ['2015-04-03']
        rank_path_file = "./page_ranks/pagerank_" + symbol
        process_one_file_for_pagerank(symbol + ".txt", period_range_pr, exclude_range, rank_path_file)
