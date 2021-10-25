# "Машинное обучение на больших данных". ДЗ №3 - Линейная регрессия на Scala  

Реализация алгоритма линейной регрессии на Scala с использованием библиотеки Breeze.  

Решение офрмлено в виде полноценного проекта на Scala. Для нахождения параметров модели используется подход,
основанный на решении нормального уравнения (normal equation:  https://machinelearningmedium.com/2017/08/28/normal-equation/).   
Для данного варианта линейной регрессии характерно более точное решение, при этом не требуется нормализации признаков.   

Для демонстрации использованы встроенные в `sklearn` данные `california_housing`:  
https://scikit-learn.org/stable/modules/generated/sklearn.datasets.fetch_california_housing.html  

Для запуска в качестве аргументов командной строки задаются название (с полным путем) обучающего и тестового файла:  
`C:\MADE\ML_BD\HW\HW3_Scala\data\train.csv C:\MADE\ML_BD\HW\HW3_Scala\data\test.csv`  
Обучающие данные делятся на собственно обучающее и валидационное подмножества. Подбор параметров производится на обучающем подмножестве, 
на валидации происходит оценка обученной линейной регрессии. После этого загружается тестовый датасет и происходит 
финальная оценка качества обучения. Предсказания модели на тестовом наборе сохраняются в файл `predictions.csv`


Пример лога программы:  
```
Train data loaded successfully
Train data is split to train and validation sets
Fitting is done!
MSE on validation set: 0.548726043346295
Test data loaded successfully
MSE on test set: 0.5317404672310238
Predictions are saved to file predictions.csv
```
