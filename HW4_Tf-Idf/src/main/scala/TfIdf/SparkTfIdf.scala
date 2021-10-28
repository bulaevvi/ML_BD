package TfIdf

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object SparkTfIdf {

  def main(args: Array[String]): Unit = {
    // Взять только 100 самых встречаемых
    val topNum = 100
    // Константа для предотвращения деления на ноль в Tf-Idf
    val smallVal = 0.001

    // Создаем сессию спарка
    val spark = SparkSession.builder()
      // Адрес мастера
      .master("local[*]")
      // Имя приложения в интерфейсе спарка
      .appName("HW4_Tf-Idf")
      // Взять текущий или создать новый
      .getOrCreate()

    // Синтаксический сахар для удобной работы со спарк
    import spark.implicits._

    // Прочитаем данные из папки data
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/tripadvisor_hotel_reviews.csv")
      // Добавим колонку с возрастающим id
      .withColumn("Review_id", monotonically_increasing_id())

    // Почистим колонку "Review" и разобьем по пробелу
    val dfReviews = df
      .select(col("Review"), col("Review_id"))
      .withColumn("Review", regexp_replace(col("Review"), """\s+""", " "))
      .withColumn("Review", regexp_replace(col("Review"), """[^\w\s]+""", ""))
      .withColumn("Review", trim(col("Review")))
      .withColumn("Review", lower(col("Review")))
      .withColumn("Review", split(col("Review"), pattern = " "))

    // Добавим столбец с количеством слов
    val dfReviewsWordCount = dfReviews
      .withColumn("Word_count", size(col("Review")))

    // Делаем explode
    val dfExplodeWords = dfReviewsWordCount
      .withColumn("Word", explode(col("Review")))

    // Рассчитаем TF
    val dfReviewWordTf = dfExplodeWords
      .groupBy("Review_id", "Word")
      .agg(
        count("Review") as "Tf",
        first("Word_count") as "Word_count")
      .withColumn("Tf", col("Tf") / col("Word_count"))

    // Рассчитаем Df и выберем topNum самых встречаемых
    val window = Window.orderBy(col("Df").desc)
    val dfWordDf = dfExplodeWords
      .groupBy("Word")
      .agg(countDistinct("Review_id") as "Df")
      .withColumn("Row_num", row_number.over(window))
      .where(col("Row_num") < topNum)
      .drop("Row_num")

    // Рассчитаем Idf
    val reviewsCount = dfReviews.count()
    val calcIdf = udf { df: Long => math.log(reviewsCount.toDouble / (df.toDouble + smallVal)) }
    val dfWordIdf = dfWordDf
      .withColumn("Idf", calcIdf(col("Df")))

    // Рассчитаем Tf-Idf
    val dfWordTfIdf = dfWordIdf
      .join(dfReviewWordTf, Seq("Word"), "left")
      .withColumn("Tf-Idf", col("Tf") * col("Idf"))

    // Запайвотим табличку
    val pivotTfIdf = dfWordTfIdf.groupBy("Review_id")
      .pivot(col("Word"))
      .agg(first(col("Tf-Idf"), ignoreNulls = true))
      .na.fill(0.0)

    // Выведем первые 10
    pivotTfIdf.show(10)

    // Сохраним результаты в файл
    pivotTfIdf
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .csv("data/result.csv")

  }

}
