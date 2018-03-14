package clustering

import myUtils.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

import scala.util.Random

/**
  * Created by lining on 10/11/17.
  */

class Particle (val id: Integer,
                        var centroids: Array[VectorWithNorm],
                        val velocity: Array[Vector],

                        val coefficient1: Double,
                        val coefficient2: Double)
  extends Serializable{ // initially, velocity should be all zero..
  private var thisBestLocalCentroids: Array[VectorWithNorm] =
    Array.fill(centroids.length)(new VectorWithNorm(Vectors.zeros(centroids.head.vector.size)))

  def updateCentroids(paraCentroids: Array[Option[VectorWithNorm]]): Particle={
    for(i <- 0 until paraCentroids.length){
      if(paraCentroids(i) != None){
        centroids(i) = paraCentroids(i).get
      }
    }
    this
  }

  private var thisBestLocalCost: Double = Double.MaxValue//??默认设成无限大！

  def update(globalBest: Array[VectorWithNorm], inertiaWeight:Double, centroidMatch: Boolean): Particle =
    centroidMatch match {
      case true => updateWithCentroidMatching(globalBest, inertiaWeight)
      case false => updateWithoutCentroidMatching (globalBest, inertiaWeight)
    }

  def updateWithoutCentroidMatching(globalBest: Array[VectorWithNorm], inertiaWeight:Double): Particle = {
    val random = scala.util.Random
    val r1 = random.nextDouble
    val r2 = random.nextDouble
    for(i <- 0 until globalBest.length)
    {
      val tempCentroid = centroids(i).vector
      val tempVelocity = velocity(i)
      val globalBestCentroid = globalBest(i).vector
      val localBestCentroid = thisBestLocalCentroids(i).vector
      val tempVector1 = new DenseVector(tempCentroid.toArray.clone)
      val tempVector2 = new DenseVector(tempCentroid.toArray.clone)
      scal(inertiaWeight, tempVelocity)
      axpy(-1.0, globalBestCentroid, tempVector1) // tempVector-globalBestCentroid
      //scal(-1.0*r1*coefficient1, tempVector)
      axpy(-1.0*r1*coefficient1, tempVector1, tempVelocity)
      axpy(-1.0, localBestCentroid, tempVector2)
      axpy(-1.0*r2*coefficient2, tempVector2, tempVelocity)
      velocity(i) = tempVelocity//
      axpy(1.0, tempVelocity, tempCentroid)
      centroids(i) = new VectorWithNorm(tempCentroid)
    }
    this
  }

  def updateWithCentroidMatching(globalBest: Array[VectorWithNorm], inertiaWeight:Double): Particle ={
    //??to be done!
    //??should I worry about the lengh of the array is different?? how to log warning or catch exception
    val random = scala.util.Random
    val r1 = random.nextDouble
    val r2 = random.nextDouble
    var indexList = List.range(0, centroids.length)
    indexList = Random.shuffle(indexList)
    val unMatchedSet= scala.collection.mutable.Set[Int]()
    for(tmp <- 0 to globalBest.length -1){
      unMatchedSet.add(tmp)
    }
    for(i <- indexList)
    {
      val index = indexList(i)
      val tempCentroid = centroids(index)
      val tempVelocity = velocity(index)
      val (matchedIndex, tmpCost) =
        ClusteringUtils.findClosest(globalBest, tempCentroid, unMatchedSet.toList)

      val tempCentroidVector = tempCentroid.vector
      val globalBestCentroidVector = globalBest(matchedIndex).vector
      val localBestCentroid = thisBestLocalCentroids(index).vector
      val tempVector1 = new DenseVector(tempCentroidVector.toArray.clone)
      val tempVector2 = new DenseVector(tempCentroidVector.toArray.clone)
      scal(inertiaWeight, tempVelocity)
      axpy(-1.0, globalBestCentroidVector, tempVector1) // tempVector-globalBestCentroid
      axpy(-1.0*r1*coefficient1, tempVector1, tempVelocity)
      axpy(-1.0, localBestCentroid, tempVector2)
      axpy(-1.0*r2*coefficient2, tempVector2, tempVelocity)
      velocity(index) = tempVelocity//
      axpy(1.0, tempVelocity, tempCentroidVector)
      centroids(index) = new VectorWithNorm(tempCentroidVector)
      unMatchedSet.remove(matchedIndex)
    }
    this
  }
  def updateLocalBest(newCost: Double): Particle = {
    if(newCost < thisBestLocalCost){
      thisBestLocalCost = newCost
      thisBestLocalCentroids = centroids.map(new VectorWithNorm(_))
      // deep copy :)
    }
    this
  }
  def getCentroids: Array[VectorWithNorm] = centroids
  def getVelocity: Array[Vector] = velocity
}
