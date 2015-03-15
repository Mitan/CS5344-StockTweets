import sys

__author__ = 'a0134673'
if __name__ == "__main__":
    input = open(sys.argv[1], 'r')
    output = open(sys.argv[2], 'w')
    lines = input.readlines()
    dates = map(lambda x: x.split()[0], lines)
    values = map(lambda x: float(x.split()[1]), lines)
    normalized_values = []
    # for first we have no diff
    normalized_values.append(0.0)
    for i in range(1, len(values)):
        normalized_values.append((values[i] - values[i-1])/ values[i-1])
    for i in range(len(values)):
        output.write("{0}\t{1}\n".format(dates[i], normalized_values[i]))
    input.close()
    output.close()
