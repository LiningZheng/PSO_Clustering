/**
  * Created by lining on 6/14/17.
  */

package clustering

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class PSOClusteringModel( val clusterCenters: Array[Vector], val trainCost:Double)
//contains just one paritle's cluster centers.
  extends Serializable{
  def this(clusterCenters: Array[Vector]) = this(clusterCenters, 0.0)
  def k :Int = clusterCentersWithNorm.length

  val clusterCentersWithNorm =
    if (clusterCenters == null) null else clusterCenters.map(new VectorWithNorm(_))

  def predict(point: Vector): Int =
    clustering.ClusteringUtils.findClosest(clusterCentersWithNorm, new VectorWithNorm(point))._1

  def predict(points: RDD[Vector]): RDD[Int] = {
    val bcCentersWithNorm = points.context.broadcast(clusterCentersWithNorm)
    points.map(p => clustering.ClusteringUtils.findClosest(bcCentersWithNorm.value, new VectorWithNorm(p))._1)
  }
  def computeCost(data: RDD[Vector]):Double ={
    val bcCentersWithNorm = data.context.broadcast(clusterCentersWithNorm)
    val cost = data.map(p=>ClusteringUtils.pointCost(bcCentersWithNorm.value, new VectorWithNorm(p))).sum()
    bcCentersWithNorm.destroy()
    cost
  }
}

