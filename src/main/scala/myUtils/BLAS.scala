/**
  * Created by lining on 6/16/17.
  */
package myUtils

import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import com.github.fommil.netlib.{F2jBLAS, BLAS => NetlibBLAS}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.linalg.Vector

object BLAS extends Serializable{
  @transient private var _f2jBLAS: NetlibBLAS = _
  @transient private var _nativeBLAS: NetlibBLAS = _

  def f2jBLAS: NetlibBLAS = { // NetlibBLAS is a parent Class of F2jBLAS
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }

  private def axpy(a: Double, x: DenseVector, y: DenseVector): Unit = {
    val n = x.size
    f2jBLAS.daxpy(n, a, x.values, 1, y.values, 1)
  }

  private def axpy(a: Double, x: SparseVector, y: DenseVector): Unit = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += xValues(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }

  def axpy(a: Double, x: Vector, y: Vector): Unit = {
    require(x.size == y.size)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            this.axpy(a, sx, dy)
          case dx: DenseVector =>
            this.axpy(a, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(
              s"axpy doesn't support x type ${x.getClass}.")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"axpy only supports adding to a dense vector but got type ${y.getClass}.")
    }
  }

  def scal(a: Double, x: Vector): Unit = {
    x match {
      case sx: SparseVector =>
        f2jBLAS.dscal(sx.values.length, a, sx.values, 1)
      case dx: DenseVector =>
        f2jBLAS.dscal(dx.values.length, a, dx.values, 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
  }

  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
        " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
    * dot(x, y)
    */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
    * dot(x, y)
    */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
    * dot(x, y)
    */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }




}
