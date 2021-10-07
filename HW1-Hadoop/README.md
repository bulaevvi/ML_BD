# "�������� �������� �� ������� ������". �� �1 - Hadoop  

## ���� 1. ������������� ���������� �������� Hadoop  

����������: ������� ���������� �������� �� ������������ ���������� � Win10 + WSL2 + Docker Desktop  


��� ���������� ����� ����� ��� ������������� ���� `docker-compose.yml`  
������� ���������� �������� `docker-compose up`

��������� ��������� ������ � Docker Desktop ��������� �� �������� Pic1 � Pic2.  

����������� � Namenode ����� ���-���������:  
`http://localhost:9870/dfshealth.html#tab-overview`  

��������� �������� �� ��������� Pic3.  

����������� � Resourcemanager ����� ���-���������:  
`http://localhost:8088/cluster`  

��������� �������� �� ��������� Pic4.  


## ���� 2. ������ � HDFS  

�������, �������������� ��� ���������� ����� 2, ��������� � ����� *hdfs_commands.txt*


## ���� 3. ��������� map reduce �� Python

��� ������ �� ���� nodemanager ������� ��������� python:  
`docker exec -it nodemanager1 /bin/bash` ����������� � ����������  
`apt update && apt upgrade && apt-get install python3` ������ ������� ���������  

��������� �������� ������ (���� AB_NYC_2019.csv.csv), ��������� � namenode � ���������� � HDFS:  
```
docker cp AB_NYC_2019.csv namenode:/
docker exec -it namenode /bin/bash
hdfs dfs -put AB_NYC_2019.csv /
```

����������� ������� ������������ �������� ������� �� namenode


hdfs dfs -put mean_reducer.py /

mapred streaming -mapper "python3 mean_mapper.py" -file /mean_mapper.py -reducer "python3 mean_reducer.py" -file /mean_reducer.py -input /AB_NYC_2019.csv -output /out  
mapred streaming -mapper "python3 var_mapper.py" -file /var_mapper.py -reducer "python3 var_reducer.py" -file /var_reducer.py -input /AB_NYC_2019.csv -output /out  

hdfs dfs -tail /out/part-00000


docker cp namenode:/out /out
