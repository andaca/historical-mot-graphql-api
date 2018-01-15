import csv
from datetime import datetime
import pickle
import sys


def read_rows(fname):
    with open(fname, 'r') as f:
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            yield row


def main():
    try:
        fname = sys.argv[1]
    except:
        raise SystemExit('Filename not provided')

    rows = read_rows(fname)
    cols = next(rows)
    data = (dict(zip(cols, r)) for r in rows)

    for d in data:
        d['first_use_date'] = datetime.strptime(d['first_use_date'], '%Y-%m-%d')
        d['test_date'] = datetime.strptime(d['test_date'], '%Y-%m-%d')

    print(data)


if __name__ == '__main__':
    main()
