import os
import sys
import pandas.io.data as web
import datetime as dt


def average(list):
    return sum(list) / float(len(list))

def normalization_price(target_list):
    l = len(target_list)
    answer = []
    for i in range(1, l):
        # measure in percent
        temp = 100.0 * (target_list[i] - target_list[i-1]) / target_list[i]
        answer.append(temp)
    return answer


def normalization_volume(target_list):
    mean = sum(target_list) / len(target_list)
    return map(lambda x: x / mean, target_list)


def normalization_volume(target_list, company_name):
    target_list_copy = list(target_list)

    comps = ["AAPL", "GOOGL"]
    # if average company perform global
    if company_name not in comps:
        av = average(target_list_copy)
        target_list_copy = map(lambda x: x / av, target_list_copy)

    else:
        l = len(target_list_copy)
        number_of_weeks = l / 5
        for i in range(number_of_weeks):
            local_max = average(target_list_copy[i * 5: (i + 1) * 5])
            target_list_copy[i * 5: (i + 1) * 5] = map(lambda y: y / float(local_max),
                                                       target_list_copy[i * 5: (i + 1) * 5])

        # if we have not integer number of weeks, need to normalize the tail
        last_few_days = l % 5
        if last_few_days != 0:
            local_max = average(target_list_copy[-last_few_days:])
            target_list_copy[-last_few_days:] = map(lambda y: y / float(local_max), target_list_copy[-last_few_days:])

    return target_list_copy



def GetCompanyData(startDate, endDate, companyName):
    try:
        f = web.DataReader(companyName, 'yahoo', startDate, endDate)
        values = f.values
        dates = f.index
    except:
        print "Cant find ", companyName
        return

    openData = [x[0] for x in values]
    closeData = [x[3] for x in values]
    volumeData = [x[4] for x in values]
    # open - close
    dayDifferenceData = [abs(x[2] - x[1]) for x in values]
    # this feature doesn't nedd normalization
    binary_diff = [1 if x[3] > x[0] else 0 for x in values]

    normalized_prices = map(lambda x: normalization_price(x), [openData, closeData])
    normalized_volumes = normalization_volume(volumeData, companyName)
    normalized_difference = normalization_volume(dayDifferenceData, companyName)
    dates = map(lambda x: x.to_datetime().strftime('%Y-%m-%d'), dates)
    return [dates]  + normalized_prices + [normalized_volumes, normalized_difference, binary_diff]


def GetCurrencyData(startDate, endDate, currencyType):
    try:
        f = web.DataReader(currencyType, 'fred', startDate, endDate)
    except:
        print "Cant find ", currencyType
        return
    curData = map(lambda x: x[0], f.values)
    return [f.index, curData]


def process_currencies(folder, start, end):
    currencies_directory = os.path.join(folder, "currencies")
    if not os.path.exists(currencies_directory):
        os.makedirs(currencies_directory)

    normalized_currencies_directory = os.path.join(currencies_directory, "normalized_currencies")
    if not os.path.exists(normalized_currencies_directory):
        os.makedirs(normalized_currencies_directory)

    currencies_data = [0,0,0]
    currencies = ["USDJPY", "EURUSD", "EURGBP"]

    """
    # usd - jpy
    dates, currencies_data[0] = GetCurrencyData(start, end, 'DEXJPUS')
    #eur - usd
    currencies_data[1] = GetCurrencyData(start, end, 'DEXUSEU')[1]
    # gbp - usd
    usd_uk_data = GetCurrencyData(start, end, 'DEXUSUK')[1]
    #gbp - eur
    currencies_data[2] = map(lambda x, y: y / x, currencies_data[1], usd_uk_data)
    dates = map(lambda x: x.to_datetime().strftime('%Y-%m-%d'), dates)
    """
    with open("./stock_data/currencies/USDJPY.txt", 'r') as f:
        dates_lines = f.readlines()

    dates = map(lambda x: x.split()[0], dates_lines)
    currencies_data[0] = map(lambda x: float(x.split()[1]), dates_lines)

    with open("./stock_data/currencies/EURUSD.txt", 'r') as f:
        dates_lines = f.readlines()
    currencies_data[1] = map(lambda x: float(x.split()[1]), dates_lines)

    with open("./stock_data/currencies/EURGBP.txt", 'r') as f:
        dates_lines = f.readlines()
    currencies_data[2] = map(lambda x: float(x.split()[1]), dates_lines)

    time_period = len(dates)

    normalized_currencies = map(lambda x: normalization_price(x), currencies_data)

    for i in range(3):
        normalized_filename = os.path.join(normalized_currencies_directory,currencies[i] + "_norm.txt")
        normalized_output_file = open(normalized_filename, 'w')

        for j in range(1, time_period):
            normalized_output_file.write("{0}\t{1}\n".format(dates[j], round(normalized_currencies[i][j - 1], 4)))
        normalized_output_file.close()


def process_companies(folder, start, end):
    companies_directory = os.path.join(folder, "companies")
    if not os.path.exists(companies_directory):
        os.makedirs(companies_directory)

    normalized_companies_directory = os.path.join(companies_directory, "normalized_companies")
    if not os.path.exists(normalized_companies_directory):
        os.makedirs(normalized_companies_directory)

    modes = ["open", "close", "volume", "difference", "binary_diff"]
    for company in companies:
        # raw_twitter_input = [dates, openData, closeData, volume, normalizedOpen, normalizedClose, normalizedVolume]
        data = GetCompanyData(start, end, company)
        dates = data[0]
        time_period = len(dates)

        for i in range(5):

            normalized_filename = os.path.join(normalized_companies_directory, company + "_norm_" + modes[i] + ".txt")
            normalized_output_file = open(normalized_filename, 'w')

            if modes[i] == "volume" or modes[i] == "difference" or modes[i] == "binary_diff":

                for j in range(time_period):
                    normalized_output_file.write("{0}\t{1}\n".format(dates[j], round((data[i + 1])[j], 4)))
            else:
                for j in range(1, time_period):
                    # raw_twitter_input[i+4] is corresponding normalized value, inde j - 1 because of the shift
                    normalized_output_file.write("{0}\t{1}\n".format(dates[j], round((data[i + 1])[j - 1], 4)))
            normalized_output_file.close()


if __name__ == "__main__":
    # IXIC is NASDAQ '^IXIC'
    #GSPC is S&P
    companies = ["AMZN", "AAPL", "BABA", "FB", "GOOGL", "YHOO",'SPY', "QQQ"]

    start_date = '2015-03-01'
    end_date = '2015-04-18'
    folder_name = './stock_data'
    """
    folder_name = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    """
    # transform dates into datetime
    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end_date, '%Y-%m-%d')

    directory = os.path.dirname(folder_name)
    if not os.path.exists(directory):
        os.makedirs(directory)

    process_companies(folder_name, start_date, end_date)

    process_currencies(folder_name, start_date, end_date)


    """
    f = web.DataReader("AAPL", 'yahoo', start_date, end_date)
    values = f.values
    dates = f.index

    print len(values)
    """