# "Машинное обучение на больших данных". ДЗ №2 - Hive  

## Блок 1. Развертывание локального Hive  

Примечание: все поднималось локально на персональном компьютере с Win10 + WSL2 + Docker Desktop  
Результаты выполнения первого блока приведены в папке **Block_1**

### 1.1. Развернуть локальный Hive в любой конфигурации  

Для выполнения этого пункта был использован образ из лекции:  
https://github.com/tech4242/docker-hadoop-hive-parquet  

Кластер запускался командой `docker-compose up`

Скриншоты командной строки и Docker Desktop приведены на рисунках Pic1 и Pic2.  

### 1.2. Подключиться к развернутому Hive с помощью любого инструмента  

#### 1.2.1. Подключение с помощью консольной утилиты Apache Beeline:  
`docker exec -it docker-hadoop-hive-server /bin/sh` подключение к контейнеру  
`/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000` подключение к серверу  

Команды Hive:  
`CREATE TABLE pokes (foo INT, bar STRING);`  создаем таблицу (встроенный пример)  
`LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;` вносим данные  
`show tables;` просмотр таблиц  
`select * from pokes limit 10;`  делаем запрос  

Результат приведен на рисунке Pic3.

#### 1.2.2. Подключение с помощью Hue:  

В составе приведенного выше дистрибутива уже содержится Hue  
Подключаемся к Hue через веб-интерфейс: `http://localhost:8888`, `user=demo; password=demo`  

Результат приведен на рисунке Pic4.  



## Блок 2. Работа с Hive  

Результаты выполнения второго блока приведены в папке **Block_2**  

### 2.1. Сделать таблицу artists в Hive и вставить туда значения  

Используем датасет  https://www.kaggle.com/pieca111/music-artists-popularity  

#### Способ 1. Загрузка файла на хостовой машине через GUI-интерфейс    
В графическом интерфейсе Hue выбрать файл на хостовой машине    

#### Способ 2. Загрузка через HDFS    
Переписываем файл в контейнер с namenode, помещаем файл в HDFS  
```
docker cp C:\Downloads\artists.csv docker-hadoop-hive-parquet-master_namenode_1:/
docker exec -it docker-hadoop-hive-parquet-master_namenode_1 /bin/bash
hdfs dfs -put /artists.csv /user
```  
Далее в GUI-интерфейсе Hue выбираем файл из HDFS  

####  Способ 3. Загрузка с помощью beeline  
Переписываем файл в контейнер с hive-server, заходим в него и подключаемся к hive-server:  
```
docker cp C:\Downloads\artists.csv docker-hadoop-hive-parquet-master_hive-server_1:/
docker exec -it docker-hadoop-hive-parquet-master_hive-server_1 /bin/bash
opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```  
Далее вводим SQL-запрос:
```
CREATE TABLE mytable(mbid STRING, artist_mb STRING, artist_lastfm STRING, country_mb STRING, country_lastfm STRING, 
tags_mb STRING, tags_lastfm STRING, listeners_lastfm INT, scrobbles_lastfm INT, ambiguous_artist STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/artists.csv' INTO TABLE mytable;
```  
Проверяем успешность операции с помощью запроса `select * from mytable limit 10;`  

Примечание: способ 1 у меня не сработал, способы 2-3 отработали нормально с одной оговоркой:  
результирующая таблица отобразилась в интерфейсе Hue примерно через полчаса


### 2.2. Используя Hive, найти нужные данные  

Команды, использованные для п.2.2, и результаты их выполнения, приведены в папке **Block_2**
