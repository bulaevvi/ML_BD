package LinearRegression

import java.io.File
import breeze.linalg._
import breeze.numerics.pow
import breeze.stats.mean

object LinearRegression {
  def main(args: Array[String]): Unit = {

    // Подготовка обучающих данных
    val trainFileName = new File(args(0))
    val trainData = csvread(trainFileName, separator = ',', skipLines = 1)
    val (trainX, trainY) = (trainData(::, 0 until trainData.cols - 1), trainData(::, -1))
    println("Train data loaded successfully")

    // Разделяем данные на обучающую и валидационную часть
    var fitX = trainX(0 to (0.8 * trainX.rows).toInt, ::)
    var valX = trainX((0.8 * trainX.rows).toInt to -1, ::)
    val fitY = trainY(0 to (0.8 * trainY.length).toInt)
    val valY = trainY((0.8 * trainY.length).toInt to -1)
    println("Train data is split to train and validation sets")

    // Создаем единичный столбец и объединяем его с матрицей признаков
    var t = DenseMatrix.ones[Double](fitX.rows, 1)
    fitX = DenseMatrix.horzcat(t, fitX)
    // Обучаем линейную регрессию с помощью Normal Equation
    val thetas = inv(fitX.t * fitX) * fitX.t * fitY
    println("Fitting is done!")

    // Используем обученную регрессию для прогнозов на валидационной выборке
    t = DenseMatrix.ones[Double](valX.rows, 1)
    valX = DenseMatrix.horzcat(t, valX)
    var predY = valX * thetas
    // Оцениваем качество предсказаний с помощью метрики MSE
    println(s"MSE on validation set: ${mean(pow((valY - predY), 2))}")

    // Подготовка тестовых данных
    val testFileName = new File(args(1))
    val testData = csvread(testFileName, separator = ',', skipLines = 1)
    var (testX, testY) = (testData(::, 0 until trainData.cols - 1), testData(::, -1))
    t = DenseMatrix.ones[Double](testX.rows, 1)
    testX = DenseMatrix.horzcat(t, testX)
    println("Test data loaded successfully")

    // Предсказание модели на тестовых данных
    predY = testX * thetas
    // Оцениваем качество предсказаний с помощью метрики MSE
    println(s"MSE on test set: ${mean(pow((testY - predY), 2))}")

    // Сохраняем результат предсказания в файл
    val predictions: DenseMatrix[Double] = DenseMatrix.zeros[Double](predY.length, 1)
    predictions(::, 0) := predY
    csvwrite(new File("data/predictions.csv"), predictions)
    println("Predictions are saved to file predictions.csv")

  }
}