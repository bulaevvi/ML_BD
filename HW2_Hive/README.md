# "�������� �������� �� ������� ������". �� �2 - Hive  

## ���� 1. ������������� ���������� Hive  

����������: ��� ����������� �������� �� ������������ ���������� � Win10 + WSL2 + Docker Desktop  
���������� ���������� ������� ����� ��������� � ����� **Block_1**

### 1.1. ���������� ��������� Hive � ����� ������������  

��� ���������� ����� ������ ��� ����������� ����� �� ������:  
https://github.com/tech4242/docker-hadoop-hive-parquet  

������� ���������� �������� `docker-compose up`

��������� ��������� ������ � Docker Desktop ��������� �� �������� Pic1 � Pic2.  

### 1.2. ������������ � ������������ Hive � ������� ������ �����������  

#### 1.2.1. ����������� � ������� ���������� ������� Apache Beeline:  
`docker exec -it docker-hadoop-hive-server /bin/sh` ����������� � ����������  
`/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000` ����������� � �������  
������� Hive:  
`CREATE TABLE pokes (foo INT, bar STRING);`  ������� ������� (���������� ������)  
`LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;` ������ ������  
`show tables;` �������� ������  
`select * from pokes limit 10;`  ������ ������  

��������� �������� �� ������� Pic3.

#### 1.2.2. ����������� � ������� Hue:  

� ������� ������������ ���� ������������ ��� ���������� Hue  
������������ � Hue ����� ���-���������: `http://localhost:8888`, user=demo password=demo  

��������� �������� �� ������� Pic4.  





## ���� 2. ������ � Hive  

���������� ���������� ������� ����� ��������� � ����� **Block_2**  

### 2.1. ������� ������� artists � Hive � �������� ���� ��������  

���������� �������  https://www.kaggle.com/pieca111/music-artists-popularity  

#### ������ 1. �������� ����� �� �������� ������ ����� GUI-���������    
� ����������� ���������� Hue ������� ���� �� �������� ������    

#### ������ 2. �������� ����� HDFS    
������������ ���� � ��������� � namenode, �������� ���� � HDFS  
```
docker cp C:\Downloads\artists.csv docker-hadoop-hive-parquet-master_namenode_1:/
docker exec -it docker-hadoop-hive-parquet-master_namenode_1 /bin/bash
hdfs dfs -put /artists.csv /user
```  
����� � GUI-���������� Hue �������� ���� �� HDFS  

####  ������ 3. �������� � ������� beeline  
������������ ���� � ��������� � hive-server, ������� � ���� � ������������ � hive-server:  
```
docker cp C:\Downloads\artists.csv docker-hadoop-hive-parquet-master_hive-server_1:/
docker exec -it docker-hadoop-hive-parquet-master_hive-server_1 /bin/bash
opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```  
����� ������ SQL-������:
```
CREATE TABLE mytable(mbid STRING, artist_mb STRING, artist_lastfm STRING, country_mb STRING, country_lastfm STRING, tags_mb STRING, tags_lastfm STRING, listeners_lastfm INT, scrobbles_lastfm INT, ambiguous_artist STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/artists.csv' INTO TABLE mytable;
```  
��������� ���������� �������� � ������� ������� `select * from mytable limit 10;`  

����������: ������ 1 � ���� �� ��������, ������� 2-3 ���������� ��������� � ����� ���������:  
�������������� ������� ������������ � ��������� Hue �������� ����� �������


### 2.2. ��������� Hive, ����� ������ ������  

�������, �������������� ��� ���������� �.2.2, � ���������� �� ����������, ��������� � ����� `hive_commands.txt`
