import csv
import sys

def get_rows(fname):
  with open(fname, 'r') as f:
    return (row for row in csv.reader(f))



def main():
  rows = tryExcept(get_rows(sys.argv[1]))
   

if __name__ == '__main__':
  main()
