import os
import sys
import pandas.io.data as web
import datetime as dt

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

    answer = [openData, closeData, volumeData]
    normalized_prices = map(lambda x: normalization_price(x), [openData, closeData])
    normalized_volumes = normalization_volume(volumeData)
    dates = map(lambda x: x.to_datetime().strftime('%Y-%m-%d'), dates)

    return [dates] + answer + normalized_prices + [normalized_volumes]


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

    # usd - jpy
    dates, currencies_data[0] = GetCurrencyData(start, end, 'DEXJPUS')
    #eur - usd
    currencies_data[1] = GetCurrencyData(start, end, 'DEXUSEU')[1]
    # gbp - usd
    usd_uk_data = GetCurrencyData(start, end, 'DEXUSUK')[1]
    #gbp - eur
    currencies_data[2] = map(lambda x, y: y / x, currencies_data[1], usd_uk_data)

    time_period = len(dates)
    dates = map(lambda x: x.to_datetime().strftime('%Y-%m-%d'), dates)
    normalized_currencies = map(lambda x: normalization_price(x), currencies_data)

    for i in range(3):
        filename = os.path.join(currencies_directory, currencies[i] + ".txt")
        output_file = open(filename, 'w')

        normalized_filename = os.path.join(normalized_currencies_directory,currencies[i] + "_norm.txt")
        normalized_output_file = open(normalized_filename, 'w')

        for j in range(time_period):
            output_file.write("{0}\t{1}\n".format(dates[j], round(currencies_data[i][j], 4)))

        for j in range(1, time_period):
            normalized_output_file.write("{0}\t{1}\n".format(dates[j], round(normalized_currencies[i][j - 1], 4)))

        output_file.close()
        normalized_output_file.close()


def process_companies(folder, start, end):
    companies_directory = os.path.join(folder, "companies")
    if not os.path.exists(companies_directory):
        os.makedirs(companies_directory)

    normalized_companies_directory = os.path.join(companies_directory, "normalized_companies")
    if not os.path.exists(normalized_companies_directory):
        os.makedirs(normalized_companies_directory)

    modes = ["open", "close", "volume"]
    for company in companies:
        # data = [dates, openData, closeData, volume, normalizedOpen, normalizedClose, normalizedVolume]
        data = GetCompanyData(start, end, company)
        dates = data[0]
        time_period = len(dates)

        for i in range(3):
            filename = os.path.join(companies_directory, company + "_" + modes[i] + ".txt")
            output_file = open(filename, 'w')

            normalized_filename = os.path.join(normalized_companies_directory, company + "_norm_" + modes[i] + ".txt")
            normalized_output_file = open(normalized_filename, 'w')

            for j in range(time_period):
                # not normalized values
                output_file.write("{0}\t{1}\n".format(dates[j], round(data[i + 1][j], 4)))

            for j in range(1, time_period):
                # data[i+4] is corresponding normalized value, inde j - 1 because of the shift
                normalized_output_file.write("{0}\t{1}\n".format(dates[j], round((data[i + 4])[j - 1], 4)))

            output_file.close()
            normalized_output_file.close()


if __name__ == "__main__":
    # IXIC is NASDAQ
    #GSPC is S&P
    companies = ["AMZN", "AAPL", "FB", "GOOGL", "QQQ", "YHOO", '^IXIC', '^GSPC']
    """
    start_date = '2012-08-28'
    end_date = '2013-04-05'
    folder_name = 'C:/Users/A0134673/Downloads/TestData/'
    """
    folder_name = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]

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