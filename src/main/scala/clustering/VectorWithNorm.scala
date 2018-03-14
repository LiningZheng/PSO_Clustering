package clustering

import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

/**
  * Created by lining on 10/10/17.
  */
class VectorWithNorm(val vector: Vector, val norm: Double) extends Serializable {
  def this(vectorWithNorm : VectorWithNorm) = this (Vectors.dense(vectorWithNorm.vector.toArray.clone), vectorWithNorm.norm)
  //deep copy

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)

  def toVector(array:Array[Double]):Vector = {
    new DenseVector(array)
  }
}
