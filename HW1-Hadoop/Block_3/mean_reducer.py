#!/usr/bin/python

"""
Reducer for calculating mean value
"""

import sys


def main():
    m_j, c_j = None, None
    
    for row in sys.stdin:
        try:
            row = row.split()
            c_k = float(row[0])
            m_k = float(row[1])
        except ValueError:
            continue
        
        if (m_j is not None) and (c_j is not None):
            m_i = (c_j * m_j + c_k * m_k) / (c_j + c_k)
            c_i = c_k + c_j
            m_j, c_j = m_i, c_i
            print(c_i, m_i)
        else:
            m_j, c_j = m_k, c_k
            

if __name__ == "__main__":
    main()

