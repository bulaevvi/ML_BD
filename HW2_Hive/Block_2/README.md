### 2.2. Используя Hive, найти:  

#### a) Исполнителя с максимальным числом скробблов:  

Вариант 1 (медленный, навеянный логикой pandas)  
`SELECT artist_lastfm FROM mytable WHERE scrobbles_lastfm IN (SELECT Max(scrobbles_lastfm) FROM mytable);`  

Вариант 2 (быстрый)  
```
SELECT artist_lastfm FROM
(SELECT artist_lastfm, scrobbles_lastfm FROM mytable ORDER BY scrobbles_lastfm DESC LIMIT 1) t;
```

#### b) Самый популярный тэг на ластфм  

Значения в колонке `tags_lastfm` - строки, в которых для каждого исполнителя перечислены теги (жанры). Их может быть произвольное
количество для произвольного пользователя.  Для выполнения этого запроса используем UDTF-функции из Hive: LATERAL VIEW EXPLODE
(см. пример из официального руководства https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView).  
Теги в строке разделяются `; ` - точкой c запятой + пробел, поэтому используется SPLIT по этой комюинации символов.
Если использовать просто пробел или просто точку с запятой, результат будет неправильный. Кроме того, для унификации
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

При выполнении этого запроса я руководствовался следующей логикой. Используя прошлый запрос, создадим таблицу `tags_table` с 10 
самыми популярными тегами. Далее (также с помощью LATERAL VIEW EXPLODE) выводим пользователей, чей тег попадает в ТОП-10, делаем группировку
(т.к. у каждого пользователя может быть несколько тегов, которые попадают в ТОП-10) и сортируем в порядке убывания популярности  
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

#### d) Любой другой инсайт на ваше усмотрение: ТОП-5 исполнителей по числу скробблов из Ирландии:

```
SELECT artist_lastfm FROM 
(SELECT artist_lastfm, scrobbles_lastfm FROM mytable WHERE country_lastfm = 'Ireland' 
ORDER BY scrobbles_lastfm DESC LIMIT 5) t;
```

Результаты выполнения команд приведены в файле **Results.pdf**
