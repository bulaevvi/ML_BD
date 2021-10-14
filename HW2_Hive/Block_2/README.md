### 2.2. ��������� Hive, �����:  

#### a) ����������� � ������������ ������ ���������:  

������� 1 (���������, ��������� ������� pandas)  
`SELECT artist_lastfm FROM mytable WHERE scrobbles_lastfm IN (SELECT Max(scrobbles_lastfm) FROM mytable);`  

������� 2 (�������)  
```
SELECT artist_lastfm FROM
(SELECT artist_lastfm, scrobbles_lastfm FROM mytable ORDER BY scrobbles_lastfm DESC LIMIT 1) t;
```

#### b) ����� ���������� ��� �� ������  

�������� � ������� `tags_lastfm` - ������, � ������� ��� ������� ����������� ����������� ���� (�����). �� ����� ���� ������������
���������� ��� ������������� ������������.  ��� ���������� ����� ������� ���������� UDTF-������� �� Hive: LATERAL VIEW EXPLODE
(��. ������ �� ������������ ����������� https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView)  
���� � ������ ����������� `; ` - ������ ������� � ��������, ������� ������������ SPLIT �� ���� ���������� ��������.
���� ������������ ������ ������ ��� ������ ����� � �������, ��������� ����� ������������. ����� ����, ��� ����������
��� ������� ���������� � Lower case. � ������� ���� ����������� � �������������� ������ (NaN), ������� ������������ �������� `tag != ''`
��������� ���������� �� ����, ��������� �� ������������ � ������� �������� � �������� ���-1 - ����� ���������� ���.
� ���������� ������ ������ ����� �������������� � �������� �������� ��� ������ ������ ����� ���������� �����.  
```
SELECT tag FROM 
(SELECT tag, COUNT(tag) AS num_of_tag FROM mytable 
LATERAL VIEW EXPLODE(SPLIT(LOWER(tags_lastfm), '; ')) tmpTable AS tag
WHERE tag != '' GROUP BY tag ORDER BY num_of_tag DESC LIMIT 1) t;
```

#### c) ����� ���������� ����������� 10 ����� ���������� ����� ������  

��� ���������� ����� ������� � ���������������� ��������� �������. ��������� ������� ������, �������� ������� `tags_table` � 10 
������ ����������� ������. ����� (����� � ������� LATERAL VIEW EXPLODE) ������� �������������, ��� ��� �������� � ���-10, ������ �����������
(�.�. � ������� ������������ ����� ���� ��������� �����, ������� �������� � ���-10) � ��������� � ������� �������� ������������  
```
SELECT artist_lastfm FROM 
(WITH tags_table as 
    (SELECT tag, COUNT(tag) AS num_of_tag FROM mytable 
    LATERAL VIEW EXPLODE(SPLIT(LOWER(tags_lastfm), '; ')) tmpTable AS tag
    WHERE tag != '' GROUP BY tag ORDER BY num_of_tag DESC LIMIT 10)
SELECT artist_lastfm, scrobbles_lastfm FROM  
    (SELECT artist_lastfm, scrobbles_lastfm, tags FROM mytable
    LATERAL VIEW EXPLODE(SPLIT(LOWER(tags_lastfm), '; ')) tags_lastfm AS tags 
    WHERE tags != '') a
WHERE tags IN (SELECT tag FROM tags_table)
GROUP BY artist_lastfm, scrobbles_lastfm
ORDER BY scrobbles_lastfm DESC LIMIT 10) t;
```

#### d) ����� ������ ������ �� ���� ����������: ���-5 ������������ �� ����� ��������� �� ��������:

SELECT artist_lastfm FROM 
(SELECT artist_lastfm, scrobbles_lastfm FROM mytable WHERE country_lastfm = 'Ireland' 
ORDER BY scrobbles_lastfm DESC LIMIT 5) t;


���������� ���������� ������ ��������� � ����� **Block_2_results.pdf**
