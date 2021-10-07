# "Машинное обучение на больших данных". ДЗ №1 - Hadoop  

## Блок 1. Развертывание локального кластера Hadoop  

Примечание: кластер поднимался локально на персональном компьютере с Win10 + WSL2 + Docker Desktop  
Результаты выполнения первого блока приведены в папке **Block_1**

Для выполнения этого блока был модифицирован файл `docker-compose.yml`  
Кластер запускался командой `docker-compose up`

Скриншоты командной строки и Docker Desktop приведены на рисунках Pic1 и Pic2.  

Подключение к Namenode через веб-интерфейс:  
`http://localhost:9870/dfshealth.html#tab-overview`  

Результат приведен на скриншоте Pic3.  

Подключение к Resourcemanager через веб-интерфейс:  
`http://localhost:8088/cluster`  

Результат приведен на скриншоте Pic4.  


## Блок 2. Работа с HDFS  

Результаты выполнения второго блока приведены в папке **Block_2**

Команды, использованные для выполнения блока 2, приведены в файле `hdfs_commands.txt`


## Блок 3. Написание map reduce на Python

Результаты выполнения третьего блока приведены в папке **Block_3**

### Установка python на nodemanager  

Для начала на всех nodemanager вручную установил python:  
`docker exec -it nodemanager1 /bin/bash` Подключение к контейнеру  
`apt update && apt upgrade && apt-get install python3` Запуск команды установки  

### Локальное тестирование маппера и редьюсера  

Перед запуском скриптов на hadoop протестируем их локально:  
`docker cp mean_mapper.py nodemanager1:/` Записываем исходные данные и скрипт на контейнер с установленным python  
`docker exec -it nodemanager1 /bin/bash` Подключение к контейнеру  
`cat AB_NYC_2019.csv | python3 mean_mapper.py | sort | python3 mean_reducer.py` Тест mapreduce для расчета среднего  

### Загрузка данных на HDFS  

Скачиваем исходные данные (файл AB_NYC_2019.csv.csv), скидываем в namenode и записываем в HDFS:  
```
docker cp AB_NYC_2019.csv namenode:/
docker exec -it namenode /bin/bash
hdfs dfs -put AB_NYC_2019.csv /
```
Аналогичным образом переписываем скрипты на namenode

### Запуск скриптов на hadoop  

Команды запуска скриптов mapreduce на кластере Hadoop для рассчета среднего и дисперсии соответственно :
```
mapred streaming -mapper "python3 mean_mapper.py" -file /mean_mapper.py -reducer "python3 mean_reducer.py" -file /mean_reducer.py -input /AB_NYC_2019.csv -output /out  
mapred streaming -mapper "python3 var_mapper.py" -file /var_mapper.py -reducer "python3 var_reducer.py" -file /var_reducer.py -input /AB_NYC_2019.csv -output /out  
```

### Анализ результатов

Выгрузка результатов на локальный компьютер:
```
hdfs dfs -get /out
docker cp namenode:/out /out
```

Результаты сравнения значений, полученных в pandas и mapreduce, приведены в файле `comparison.txt`
Как показывает сравнение, отличия наблюдаются только c десятого знака после запятой.  



Полезные ссылки:  
https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html  
https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
