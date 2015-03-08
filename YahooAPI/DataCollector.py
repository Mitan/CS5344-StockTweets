import sys
import pandas.io.data as web
import datetime as dt


def GetData(startDate, endDate, companyName, mode):
    start = dt.datetime.strptime(startDate, '%Y-%m-%d')
    end = dt.datetime.strptime(endDate, '%Y-%m-%d')

    try:
        f = web.DataReader(companyName, 'yahoo', start, end)
        values = f.values
        dates = f.index
    except:
        print "Cant find ", companyName
        return

    if mode == 'C':
        modeIndex = 3
    elif mode == 'V':
        modeIndex = 4
    else:
        raise Exception("Imcorrect mode")
    dates = map(lambda x : x.to_datetime().strftime('%Y-%m-%d'), dates)
    answer = map(lambda x: round(x[modeIndex], 2), values)
    return zip(dates, answer)


if __name__ == "__main__":
    """
    #format
    startV = '2012-08-01'
    endV = '2012-08-05'
    companyName = "GOOGL"
    mode = 'C'
    filename = 'C:\\Users\A0134673\\Downloads\\testfile.txt'
    data = GetData(startV, endV, companyName, mode)
    """
    data = GetData(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    file = open(sys.argv[5], 'w')
    for item in data:
        file.write("{0}\t{1}\n".format(item[0], item[1]))
    file.close()
