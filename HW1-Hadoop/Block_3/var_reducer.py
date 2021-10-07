#!/usr/bin/python

"""
Reducer for calculating variance
"""

import sys


def main():
    m_j, c_j, v_j = None, None, None
    
    for row in sys.stdin:
        try:
            row = row.split()
            c_k = float(row[0])
            m_k = float(row[1])
            v_k = float(row[2])
        except ValueError:
            continue
        
        if (m_j is not None) and (c_j is not None):
            m_i = (c_j * m_j + c_k * m_k) / (c_j + c_k)
            c_i = c_k + c_j
            v_i = ((c_j * v_j + c_k * v_k) / (c_j + c_k)) + c_j * c_k * ((m_j - m_k) / (c_j + c_k)) ** 2
            m_j, c_j, v_j = m_i, c_i, v_i
            print(c_i, m_i, v_i)
        else:
            m_j, c_j, v_j = m_k, c_k, v_k
            

if __name__ == "__main__":
    main()

