# -*- coding: utf-8 -*-
"""
Расчет среднего и дисперсии с помощью pandas
"""

import pandas as pd

df = pd.read_csv('AB_NYC_2019.csv')

mean = df['price'].mean(skipna = True)
var = df['price'].var(skipna = True, ddof=0)

print(mean, var)