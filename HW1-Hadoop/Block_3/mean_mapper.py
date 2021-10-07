#!/usr/bin/python

"""
Mapper for calculating mean value
"""

import sys
import csv


TARGET = 9  # Column wih needed data


def main():
    input_stream = csv.reader(sys.stdin)
    for row in input_stream:
        print("1", row[TARGET])
    

if __name__ == "__main__":
    main()
    
