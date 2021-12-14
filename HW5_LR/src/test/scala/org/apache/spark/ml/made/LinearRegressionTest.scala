package org.apache.spark.ml.made


import breeze.linalg.DenseVector
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, rand}
import com.google.common.io.Files
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


class LinearRegressionTest extends AnyFlatSpec with should.Matchers with WithSpark {

  lazy val vectors: Seq[Vector] = LinearRegressionTest._vectors
  lazy val data: DataFrame = LinearRegressionTest._data
  lazy val train_data: DataFrame = LinearRegressionTest._train_data
  lazy val big_data: DataFrame = LinearRegressionTest._big_data
  val delta: Double = 0.001
  val stepSize: Double = 0.8
  val numIterations: Int = 200
  // Вектор весов "скрытой модели"
  val weights: Vector = Vectors.dense(1.5, 0.3, -0.7)
  val bias: Double = -1

  // "Маленький" тест для ручной проверки правильности математических операций
  "Model" should "return linear combination of the features" in {
    val model: LinearRegressionModel = new LinearRegressionModel(
      weights = weights.toDense,
      bias = bias
    ).setInputCol("features")
      .setOutputCol("prediction")

    val values = model.transform(data).collect().map(_.getAs[Double](1))

    values.length should be(data.count())

    values(0) should be(vectors(0)(0) * weights(0) + vectors(0)(1) * weights(1) + vectors(0)(2) * weights(2) + bias +- delta)
    values(1) should be(vectors(1)(0) * weights(0) + vectors(1)(1) * weights(1) + vectors(1)(2) * weights(2) + bias +- delta)
  }

  // Нагрузочный тест для проверки правильности обучения "скрытой модели"
  "Estimator" should "produce needed model" in {
    // Создаем истинную "скрытую модель"
    val true_model: LinearRegressionModel = new LinearRegressionModel(
      weights = weights.toDense,
      bias = bias
    ).setInputCol("features")
      .setOutputCol("label")

    // Вычисляем истинные значения целевой переменной путем подачи на вход "скрытой модели" случайной матрицы
    val true_target = true_model
      .transform(big_data)
      .select(col("features"), col("label"))

    // Создаем estimator и обучаем его на случайном массиве и истинных метках
    val estimator: LinearRegression = new LinearRegression(stepSize, numIterations)
      .setInputCol("features")
      .setOutputCol("label")
    val model = estimator.fit(true_target)

    // Сравниваем полученные веса со "скрытой моделью"
    model.weights(0) should be(weights(0) +- delta)
    model.weights(1) should be(weights(1) +- delta)
    model.weights(2) should be(weights(2) +- delta)
    model.bias should be(bias +- delta)
  }

  "Model" should "work after re-read" in {
    val true_model: LinearRegressionModel = new LinearRegressionModel(
      weights = weights.toDense,
      bias = bias
    ).setInputCol("features")
      .setOutputCol("label")

    val true_target = true_model
      .transform(train_data.select(col("features")))
      .select(col("features"), col("label"))

    var pipeline = new Pipeline().setStages(Array(
      new LinearRegression(stepSize, numIterations)
        .setInputCol("features")
        .setOutputCol("label")
    ))

    val tmpFolder = Files.createTempDir()
    pipeline.fit(true_target).write.overwrite().save(tmpFolder.getAbsolutePath)

    val pipeline_model = PipelineModel.load(tmpFolder.getAbsolutePath)
    val model = pipeline_model.stages(0).asInstanceOf[LinearRegressionModel]

    model.getInputCol should be("features")
    model.getOutputCol should be("label")
    model.weights(0) should be(weights(0) +- delta)
    model.weights(1) should be(weights(1) +- delta)
    model.weights(2) should be(weights(2) +- delta)
    model.bias should be(bias +- delta)
  }

  "Estimator" should "work after re-read" in {
    val true_model: LinearRegressionModel = new LinearRegressionModel(
      weights = weights.toDense,
      bias = bias
    ).setInputCol("features")
      .setOutputCol("label")

    val true_target = true_model
      .transform(train_data.select(col("features")))
      .select(col("features"), col("label"))

    var pipeline = new Pipeline().setStages(Array(
      new LinearRegression(stepSize, numIterations)
        .setInputCol("features")
        .setOutputCol("label")
    ))

    val tmpFolder = Files.createTempDir()
    pipeline.write.overwrite().save(tmpFolder.getAbsolutePath)

    pipeline = Pipeline.load(tmpFolder.getAbsolutePath)
    pipeline.getStages(0).asInstanceOf[LinearRegression].stepSize should be(stepSize)
    pipeline.getStages(0).asInstanceOf[LinearRegression].numIterations should be(numIterations)

    val model = pipeline.fit(true_target).stages(0).asInstanceOf[LinearRegressionModel]

    model.weights(0) should be(weights(0) +- delta)
    model.weights(1) should be(weights(1) +- delta)
    model.weights(2) should be(weights(2) +- delta)
    model.bias should be(bias +- delta)
  }
}

object LinearRegressionTest extends WithSpark {
  import sqlc.implicits._

  // Создаем тестовые векторы и объединяем их в датафрейм для smalltest
  lazy val _vectors = Seq(
    Vectors.dense(-1.9, -0.2, 1.3),
    Vectors.dense(-2.1,  0.4, 1.6),
  )
  lazy val _data: DataFrame = {
    _vectors.map(x => Tuple1(x)).toDF("features")
  }

  // Создаем небольшое количество случайных векторов для теста загрузки-выгрузки и закидываем их в датафрейм
  lazy val _train_points: Seq[Vector] = Seq.fill(100)(Vectors.fromBreeze(DenseVector.rand(3)))
  lazy val _train_data: DataFrame = {
    _train_points.map(x => Tuple1(x)).toDF("features")
  }

  // Создаем большое количество случайных векторов для нагрузочного теста и закидываем их в датафрейм размера 100_000 х 3
  lazy val _big_points: Seq[Vector] = Seq.fill(100000)(Vectors.fromBreeze(DenseVector.rand(3)))
  lazy val _big_data: DataFrame = {
    _big_points.map(x => Tuple1(x)).toDF("features")
  }
}