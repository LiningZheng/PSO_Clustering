package clustering

import myUtils.MyUtils

/**
  * Created by lining on 11/10/17.
  */
object ClusteringUtils {

  def fastSquaredDistance(
                           v1: VectorWithNorm,
                           v2: VectorWithNorm): Double = {
    MyUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

  private[clustering] def findClosest(
                                       centers: TraversableOnce[VectorWithNorm],
                                       point: VectorWithNorm
                                     ):(Int, Double)={
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach{ center =>
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }
  private[clustering] def findClosest(
                                       centers: Array[VectorWithNorm],
                                       point: VectorWithNorm,
                                       indexList:List[Int]
                                     ):(Int, Double)={
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    indexList.foreach{ index =>
      val center = centers(index)
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = index
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  def pointCost(centers:Array[VectorWithNorm], vectorWithNorm:VectorWithNorm): Double =
    findClosest(centers,vectorWithNorm)._2
}
