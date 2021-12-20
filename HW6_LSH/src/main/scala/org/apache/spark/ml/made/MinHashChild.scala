package org.apache.spark.ml.made

import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, LSH, LSHModel, MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{IntParam, ParamValidators, Params}
import org.apache.spark.mllib.util.MLUtils

import scala.util.Random

class MinHashChildModel(override val uid: String,
                        override val randCoefficients: Array[(Int, Int)],
                        bandWidth: Int)
  extends MinHashLSHModel(uid, randCoefficients) {
  override protected[ml] def hashFunction(elems: Vector): Array[Vector] = {
    require(elems.nonZeroIterator.nonEmpty, "Must have at least 1 non zero entry.")
    val hashValues = randCoefficients.map { case (a, b) =>
      elems.nonZeroIterator.map { case (i, _) =>
        ((1L + i) * a + b) % MinHashLSH.HASH_PRIME
      }.min.toDouble
    }
    hashValues.grouped(bandWidth).map(Vectors.dense).toArray
  }
}

trait ChildParams extends Params {
  final val bandWidth: IntParam = new IntParam(this, "bandWidth", "", ParamValidators.gt(0))
  final def getBandWidth: Int = $(bandWidth)

  setDefault(bandWidth -> 3)
}

class MinHashChild extends MinHashLSH with ChildParams {
  def setBandWidth(value: Int): this.type = set(bandWidth, value)
  override protected[ml] def createRawLSHModel(inputDim: Int): MinHashLSHModel = {
    require(inputDim <= MinHashLSH.HASH_PRIME,
      s"The input vector dimension $inputDim exceeds the threshold ${MinHashLSH.HASH_PRIME}.")
    val rand = new Random($(seed))
    val randCoefs: Array[(Int, Int)] = Array.fill($(numHashTables)) {
      (1 + rand.nextInt(MinHashLSH.HASH_PRIME - 1), rand.nextInt(MinHashLSH.HASH_PRIME - 1))
    }
    new MinHashChildModel(uid, randCoefs, $(bandWidth))
  }
}


// Доопределение методов для класса из семинара
class TestModel (override val uid: String,
                  private[made] val randHyperPlanes: Array[Vector]
                ) extends LSHModel[TestModel] {

  override protected[ml] def hashFunction(elems: linalg.Vector): Array[linalg.Vector] = {
    val hashValues = randHyperPlanes.map(
      randHyperPlane => if (elems.dot(randHyperPlane) >= 0) 1 else -1
    )
    hashValues.map(Vectors.dense(_))
  }

  override protected[ml] def keyDistance(x: linalg.Vector, y: linalg.Vector): Double = {
    if (Vectors.norm(x, 2) == 0 || Vectors.norm(y, 2) == 0){
      1.0
    } else {
      1.0 - x.dot(y) / (Vectors.norm(x, 2) * Vectors.norm(y, 2))
    }
  }

  override protected[ml] def hashDistance(x: Seq[linalg.Vector], y: Seq[linalg.Vector]): Double = {
    x.zip(y).map(item => if (item._1 == item._2) 1 else 0).sum.toDouble / x.size
  }
}


// Доопределение методов для класса из семинара
class Test extends LSH[TestModel] {

  override protected[this] def createRawLSHModel(inputDim: Int): TestModel = {
    val rand = new Random(0)
    val randHyperPlanes: Array[Vector] = {
      Array.fill($(numHashTables)) {
        val randArray = Array.fill(inputDim)({if (rand.nextGaussian() > 0) 1.0 else -1.0})
        linalg.Vectors.fromBreeze(breeze.linalg.Vector(randArray))
      }
    }
    new TestModel(uid, randHyperPlanes)
  }
}