/**
  * Created by lining on 6/14/17.
  */
package clustering
//import org.apache.spark.internal.Logging
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import myUtils.Logging
import scala.util.Random
import scala.util.control.Breaks._
import myUtils.BLAS.{axpy, scal}
import myUtils.MyUtils

//import clustering.PSOClusteringModel

class PSOClustering private( //the private indicates that it's a private constructor.
                             protected  var k :Int = 2,
                             protected  var maxIterations: Int,
                             protected var swarmSize: Int,
                             protected var epsilon: Double,
                             protected var minInertiaWeight: Double,
                             protected var maxInertiaWeight: Double,
                             protected var coefficient1: Double,
                             protected var coefficient2: Double,
                             protected var seed: Long,
                             protected var centroidMatching:Boolean
                           )extends Serializable with Logging{

  def this() = this(2, 20, 5, 1e-4, 0.4, 0.9, 2.0, 2.0, (new scala.util.Random).nextLong(),false)
  def getK: Int = k
  def setK (k:Int): this.type = {
    require(k>0, s"Number of cluster must be positive but got ${k}")
    this.k = k
    this
  }


  def setMaxIterations (maxIterations:Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  def setSwarmSize(size: Int): this.type = {
    require(size >0, s"Swarm size must be positive but got ${size}")
    this.swarmSize = size
    this
  }

  def setEpsilon(epsilon: Double): this.type = {
    require(epsilon >= 0,
      s"Distance threshold must be nonnegative but got ${epsilon}")
    this.epsilon = epsilon
    this
  }
  def setMaxInertiaWeight (inertiaWeight: Double): this.type ={
    require(inertiaWeight > 0, s"inertiaWeight must be positive but got ${inertiaWeight}")
    this.maxInertiaWeight= inertiaWeight
    this
  }
  def setMinInertiaWeight (inertiaWeight: Double): this.type ={
    require(inertiaWeight > 0, s"inertiaWeight must be positive but got ${inertiaWeight}")
    this.minInertiaWeight = inertiaWeight
    this
  }
  def setCoefficients(coef1:Double, coef2:Double): this.type = {
    require(coef1 > 0 && coef2 >0, s"both coefficients must be positive but got ${coef1} and ${coef2}")
    this.coefficient1 = coef1
    this.coefficient2 = coef2
    this
  }
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }
  def setCentroidMatching(cm:Boolean): this.type={
    this.centroidMatching = cm
    this
  }
  protected var initialModel: Option[PSOClusteringModel] = None //??PSOClusteringModel needs to be defined

  def setInitialModel(model: PSOClusteringModel):this.type ={
    require(model.k==k, s"the initial model must have the same k!")
    initialModel = Some(model)
    this
  }

  protected def run(//?? I didn't use Instrumentation because I don't know what it is lol
                    data: RDD[Vector]): PSOClusteringModel = {


    log.info("Testing logging :)")
    if (data.getStorageLevel == StorageLevel.NONE) {
      /* logWarning("The input data is not directly cached, which may hurt performance if its"
         + " parent RDDs are also uncached.")*/
      print("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    // Compute squared norms and cache them.
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map { case (v, norm) =>
      new VectorWithNorm(v, norm)
    }
    val model = runAlgorithm(zippedData)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      /*logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")*/
      print("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  protected  def runAlgorithm(
                               data: RDD[VectorWithNorm]): PSOClusteringModel={
    val sc = data.sparkContext
    val initStartTime = System.nanoTime()
    //the default initialization is random initialilzation. I'll add more later.
    val swarm = initRandom(data)
    initialModel match {
      case Some(model) => {
        val tpCentroids = model.clusterCentersWithNorm.map(new VectorWithNorm(_))//deep copy 666
        swarm(0) = new Particle(0,tpCentroids,
          Array.fill(tpCentroids.length)(Vectors.zeros(tpCentroids.head.vector.size)),
          this.coefficient1, this.coefficient2)
      }
      case None => {}
    }

    val dataSize = data.count()
    var iteration = 0
    var converged = false
    var paralellizedSwarm = sc.parallelize(swarm).cache()//?? how to define granularity. how many partitions?
    var bestGlobalCentroids : Array[VectorWithNorm] = null
    var bestCost = Double.PositiveInfinity
    var resultBestGlobalCentroids : Array[VectorWithNorm] = null
    var resultCost = Double.PositiveInfinity
    val iterationStartTime = System.nanoTime()

    while(iteration < maxIterations && !converged){
      val totalParticleCentroids = paralellizedSwarm.map(_.getCentroids).collect()
      val bcParticles = sc.broadcast(totalParticleCentroids)//Array[Array[Vector]]
      val totalContribs = data.mapPartitions{ points =>
        val thisParticles = bcParticles.value
        points.flatMap{ point=>
          for(i <- 0 until thisParticles.length) yield {//?? move centroids to the for parenthesis?
          val centroids = thisParticles(i)
            val (bestCenter, cost) = ClusteringUtils.findClosest(centroids,point)
            (i, cost)
          }
          //?even if the Array[Particle] is distributed, the elements are still indexed the same way as the local one, right?
        }
      }.reduceByKey{case (a,b)=>a+b}
        .mapValues{case a=>a/dataSize}
        .collectAsMap()

      bcParticles.destroy()

      var bestIndex = -1
      bestCost = Double.PositiveInfinity
      totalContribs.foreach{ case (particleIndex, fitnessValue)=>
        println(s"particleIndex: $particleIndex, fitness value: $fitnessValue")

        if(fitnessValue < bestCost)
        {
          bestIndex = particleIndex
          bestCost = fitnessValue
        }
      }
      println(s"iteration #: $iteration  \n best cost: $bestCost")


      //?? test if bestIndex==-1, if so, throw an exception.
      val optionCentroid: Option[Array[VectorWithNorm]] = totalParticleCentroids.lift(bestIndex)
      val newBestGlobalCentroids = if(optionCentroid == None) null else optionCentroid.get
      if(newBestGlobalCentroids == null){
        throw new Exception("newBestGlobalCentroids is null!!!")
      }


      if(bestGlobalCentroids != null){
        converged = true
        for(i <- 0 until newBestGlobalCentroids.length){
          if(converged && ClusteringUtils.fastSquaredDistance(newBestGlobalCentroids(i),bestGlobalCentroids(i))>  epsilon * epsilon){
            converged = false
          }
        }
      }

      bestGlobalCentroids = newBestGlobalCentroids
      if(bestCost < resultCost){
        resultBestGlobalCentroids = bestGlobalCentroids
        resultCost = bestCost
      }
      iteration += 1
      //?? shall I broadcast bestGlobalCentroids? maybe not?
      if(!converged && iteration < maxIterations){
        val currentInertiaWeight:Double =
          maxInertiaWeight-((maxInertiaWeight - minInertiaWeight)*iteration/maxIterations)
        val oldParalellizedSwarm = paralellizedSwarm
        paralellizedSwarm = oldParalellizedSwarm
          .map{particle=>
            particle.updateLocalBest(totalContribs(particle.id))
          }
          .map{ particle=>
            particle.update(bestGlobalCentroids, currentInertiaWeight, this.centroidMatching)
          }
          .cache()
        oldParalellizedSwarm.unpersist()
      }
    }
    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    //logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")
    println(f"Iterations took $iterationTimeInSeconds%.3f seconds.")
    if (iteration == maxIterations) {
      //logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
      println(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      //logInfo(s"KMeans converged in $iteration iterations.")
      println(s"KMeans converged in $iteration iterations.")
    }
    println("total iteration time:"+ (System.nanoTime() - initStartTime))
    new PSOClusteringModel(resultBestGlobalCentroids.map(_.vector), resultCost)
  }

  protected  def initRandom(data: RDD[VectorWithNorm]): Array[Particle] = {
    //?? to be continued!!
    val res = new Array[Particle](this.swarmSize)
    for(i <- 0 until swarmSize) {
      val centroids = data.takeSample(false, k, new scala.util.Random().nextInt())
      //.map(new VectorWithNorm(_))//deep copy
      // ?? what if there are not as many centroids? so maybe i shouldn't use distinct
      res(i) = new Particle(i, centroids,
        Array.fill(centroids.length)(Vectors.zeros(centroids.head.vector.size)),
        this.coefficient1, this.coefficient2)
    }
    res
  }
}

/**
  * Top-level methods for calling K-means clustering by users.
  */

object PSOClustering{
  def getRandomModel(data: RDD[Vector],
                     k:Int
                    ): PSOClusteringModel  ={
    val centroids = data.
      takeSample(false, k, new scala.util.Random().nextInt())
      .map(p=> Vectors.dense(p.toArray.clone))//deep copy

    new PSOClusteringModel(centroids)
  }
  def train( // This is used as a static function.
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String,
             seed: Long): PSOClusteringModel = {//??not finished
    null
  }

  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String): PSOClusteringModel = {//without Seed
    //??not finished
    /* new PSOClustering().set(k)
       .setMaxIterations(maxIterations)
       .setInitializationMode(initializationMode)
       .run(data)*/
    null
  }

  def train(
             data: RDD[Vector],
             k:Int,
             maxIterations: Int,
             swarmSize: Int,
             //epsilon: Double,
             minInertiaWeight: Double,
             maxInertiaWeight: Double,
             coefficient1: Double,
             coefficient2: Double
           ): PSOClusteringModel = {
    new PSOClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setSwarmSize(swarmSize)
      .setMinInertiaWeight(minInertiaWeight)
      .setMaxInertiaWeight(maxInertiaWeight)
      .setCoefficients(coefficient1, coefficient2)
      .run(data)
  }

  def train( data: RDD[Vector],
             k:Int,
             maxIterations: Int,
             swarmSize: Int
           ): PSOClusteringModel = {
    new PSOClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setSwarmSize(swarmSize)
      .run(data)
  }

  def train( data: RDD[Vector],
             model: PSOClusteringModel,
             k:Int,
             maxIterations: Int,
             swarmSize: Int
           ): PSOClusteringModel = {
    new PSOClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setSwarmSize(swarmSize)
      .setInitialModel(model)
      .run(data)
  }

  def train( data: RDD[Vector],
             model: PSOClusteringModel,
             k:Int,
             maxIterations: Int,
             swarmSize: Int,
             centroidMatching:Boolean
           ): PSOClusteringModel = {
    new PSOClustering().setK(k)
      .setMaxIterations(maxIterations)
      .setSwarmSize(swarmSize)
      .setInitialModel(model)
      .setCentroidMatching(centroidMatching)
      .run(data)
  }



}







