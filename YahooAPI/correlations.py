import math


def average(target_list):
    return sum(target_list) / float(len(target_list))


def calculate_norm(target_list, mean_of_list, lag):
 #   reduced_list = target_list[lag:] if lag >= 0 else target_list[:lag]

    return math.sqrt(sum([(x - mean_of_list) * (x - mean_of_list) for x in target_list]))


def calculate_correlation(twitter_features, stock_prices, is_volume_type, lag):
    x_data = open(twitter_features, 'r').readlines()

    # if it is a case of volume, no need to prune one day for features
    if not is_volume_type:
        x_data = x_data[1:]
    x_values = map(lambda x: float(x.split()[1]), x_data)

    y_data = open(stock_prices, 'r').readlines()
    y_values = map(lambda x: float(x.split()[1]), y_data)

    # from this moment we just have x and y and can use the formula for cross-corelation coefficient

    #ugly hack
    #todo fix
    # for page rank we have twitter features for not all the month
    len_x = len(x_values)
    len_y = len(y_values)
    diff = len_y - len_x
    if diff > 0:
        y_values = y_values[: - diff]

    # for debug
    if len(x_values) != len(y_values):
        raise Exception(
            "dimensions of raw_twitter_input are not equal for " + stock_prices + " " + str(len(x_values)) + " " + str(
                len(y_values)))
        return
    length = len(x_values)

    mean_x = sum(x_values) / float(length)
    mean_y = sum(y_values) / float(length)
    #Y lag
    if(lag > 0):
		reduced_list_x = x_values[lag:]
		reduced_list_y = y_values[:-lag]
    elif (lag < 0):
		reduced_list_x = x_values[:lag]
		reduced_list_y = y_values[-lag:]		
    else:
		reduced_list_x = x_values[:]
		reduced_list_y = y_values[:]
    new_mean_x = sum(reduced_list_x)/float(len(reduced_list_x))
    new_mean_y = sum(reduced_list_y)/float(len(reduced_list_y))
    
    #reduced_list = target_list[lag:] if lag >= 0 else target_list[:lag]
    
    norm_x = calculate_norm(reduced_list_x, new_mean_x, 0)
    norm_y = calculate_norm(reduced_list_y, new_mean_y, lag)
   # range_for_calculation = range(lag, length) if lag >= 0 else range(length + lag)
    coefficient_list = [(reduced_list_x[i] - new_mean_x) * (reduced_list_y[i] - new_mean_y) for i in range(0,len(x_values)-abs(lag))]

    correlation = sum(coefficient_list) / (norm_x * norm_y)
    return correlation


def process_correlations(partial_path_to_twitter_features, output_file_path, is_pagerank):
    correlations_file = open(output_file_path, 'w')
    companies = ["AMZN", "BABA", "FB",  "YHOO"] if is_pagerank \
        else ["AMZN", "AAPL", "BABA", "FB", "GOOGL", "YHOO", "SPY", "QQQ"]
    currencies = ["EURUSD"] if is_pagerank else ["USDJPY", "EURUSD", "EURGBP"]
    lags = range(-3, 4, 1)

    for lag in lags:
        answer_companies = []
        correlations_file.write("lag = " + str(lag) + "\n")

        for currency in currencies:
            tweets_features_file = partial_path_to_twitter_features + currency + ".txt"
            currencies_directory = "./stock_data/currencies/normalized_currencies/"
            price = calculate_correlation(tweets_features_file, currencies_directory + currency + "_norm.txt", False,
                                          lag)
            correlations_file.write(currency + "\t" + str(price) + '\n')

        for company in companies:
            tweets_features_file = partial_path_to_twitter_features + company + ".txt"
            companies_directory = "./stock_data/companies/normalized_companies/"

            close_price = calculate_correlation(tweets_features_file, companies_directory + company + "_norm_close.txt",
                                                False, lag)
            open_price = calculate_correlation(tweets_features_file, companies_directory + company + "_norm_open.txt",
                                               False, lag)
            volume = calculate_correlation(tweets_features_file, companies_directory + company + "_norm_volume.txt",
                                           True, lag)
            difference = calculate_correlation(tweets_features_file,
                                               companies_directory + company + "_norm_difference.txt",
                                               True, lag)

            answer_companies.append([company, open_price, close_price, volume, difference])
            correlations_file.write(
                company + "\t" + str(open_price) + "\t" + str(close_price) + "\t" + str(volume) + "\t" + str(
                    difference) + '\n')

        average_close = average([v[2] for v in answer_companies])
        average_open = average([v[1] for v in answer_companies])
        average_volume = average([v[3] for v in answer_companies])
        average_diff = average([v[4] for v in answer_companies])
        correlations_file.write(
            "Average\t" + str(average_open) + "\t" + str(average_close) + "\t" + str(average_volume) + "\t" + str(
                average_diff)
            + '\n')
    correlations_file.close()


process_correlations("./twitter_data/weighted_pr_users/weighted_pr_users_", './calculated_correlations/weight_pr_correlations.txt', True)
process_correlations("./twitter_data/average_pr/average_pr_", './calculated_correlations/average_pr_correlations.txt', True)
process_correlations("./twitter_data/counts/counts_", './calculated_correlations/count_correlations.txt', False)
process_correlations("./twitter_data/users/users_", './calculated_correlations/users_correlations.txt', False)
process_correlations("./twitter_data/weighted_users/weighted_users_",
                     './calculated_correlations/weighted_users_correlations.txt', False)
