### 2.2. Используя Hive, найти:  

#### a) Исполнителя с максимальным числом скробблов:  

```
SELECT artist_lastfm FROM
(SELECT artist_lastfm, scrobbles_lastfm FROM mytable ORDER BY scrobbles_lastfm DESC LIMIT 1) t;
```

#### b) Самый популярный тэг на ластфм  

Значения в колонке `tags_lastfm` - строки, в которых для каждого исполнителя перечислены теги (жанры). Их может быть произвольное
количество для произвольного пользователя.  Для выполнения этого запроса используем UDTF-функции из Hive: LATERAL VIEW EXPLODE
(см. пример из официального руководства https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView).  
Теги в строке разделяются `; ` - точкой c запятой + пробел, поэтому используется SPLIT по этой комюинации символов.
Если использовать просто пробел или просто точку с запятой, результат может быть некорректный. Кроме того, для унификации
все символы приводятся к lower case. В таблице есть исполнители с отсутствующими тегами (NaN), поэтому осуществляем проверку `tag != ''`
Результат группируем по тэгу, сортируем по популярности в порядке убывания и выбираем ТОП-1 - самый популярный тег.
В дальнейшем данный запрос будет использоваться в качестве базового для отбора десяти самых популярных тегов.  
```
SELECT tag FROM 
(SELECT tag, COUNT(tag) AS num_of_tag FROM mytable 
LATERAL VIEW EXPLODE(SPLIT(LOWER(tags_lastfm), '; ')) tmpTable AS tag
WHERE tag != '' GROUP BY tag ORDER BY num_of_tag DESC LIMIT 1) t;
```

#### c) Самые популярные исполнители 10 самых популярных тегов ластфм  

При выполнении этого запроса я руководствовался следующей логикой. Используя запрос из предыдущей задачи, создадим таблицу `tags_table` с 10 
самыми популярными тегами. Далее (также с помощью LATERAL VIEW EXPLODE) выводим пользователей, чей тег попадает в ТОП-10. Т.к. у очень популярных 
исполнителей может быть несколько тегов, которые попадут в ТОП-10, строим таблицу `result`, в которую занесем всех тех исполнителей, 
чьи теги попадают в ТОП-10. Далее из этой таблицы для каждого тега выбирается исполнитель с максимальным числом просмотров.   

```
WITH tags_table as 
    (SELECT tag, COUNT(tag) AS num_of_tag FROM mytable 
    LATERAL VIEW EXPLODE(SPLIT(LOWER(tags_lastfm), '; ')) tmpTable AS tag
    WHERE tag != '' GROUP BY tag ORDER BY num_of_tag DESC LIMIT 10),
result AS
    (SELECT artist_lastfm, scrobbles_lastfm, tags FROM  
        (SELECT artist_lastfm, scrobbles_lastfm, tags FROM mytable
        LATERAL VIEW EXPLODE(SPLIT(LOWER(tags_lastfm), '; ')) tags_lastfm AS tags 
        WHERE tags != '') a
    WHERE tags IN (SELECT tag FROM tags_table))
SELECT artist_lastfm, max_scrobbles, tags
FROM (SELECT 
        artist_lastfm,
        MAX(scrobbles_lastfm) OVER (PARTITION BY tags) max_scrobbles,
        tags,
        ROW_NUMBER() OVER (PARTITION BY tags ORDER BY scrobbles_lastfm DESC) row_num
      FROM result) r
WHERE row_num = 1 ORDER BY max_scrobbles DESC;
```

#### d) Любой другой инсайт на ваше усмотрение: 


Исполнитель с максимальным числом скробблов (pandas-style):  
`SELECT artist_lastfm FROM mytable WHERE scrobbles_lastfm IN (SELECT MAX(scrobbles_lastfm) FROM mytable);`  


ТОП-5 исполнителей по числу скробблов из Ирландии:  

```
SELECT artist_lastfm FROM 
(SELECT artist_lastfm, scrobbles_lastfm FROM mytable WHERE country_lastfm = 'Ireland' 
ORDER BY scrobbles_lastfm DESC LIMIT 5) t;
```

Результаты выполнения запросов приведены в файле **Results.pdf**
