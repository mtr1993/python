from itertools import islice

if __name__ == '__main__':
    N = 2
    X = 5
with open('/Users/mtr/PycharmProjects/mongoQuery/resource/exception/kafka.log') as f_input:
    for row in islice(f_input, N, None):
        print(row.strip())
