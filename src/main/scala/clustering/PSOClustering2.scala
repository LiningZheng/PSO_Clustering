package clustering

/**
  * Created by lining on 11/10/17.
  * PSOClustering2 evaluate the particle solutions seperately with mapreduce in an
  * iterative way instead of creating keys as (Particle ID, centroid ID)
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Vector, Vectors}


class PSOClustering2 extends PSOClustering{
  override def runAlgorithm(data:RDD[VectorWithNorm]):PSOClusteringModel={
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
      // Evaluation and pick the best particle in the swarm.
      var bestIndex = -1
      var bestCost = Double.PositiveInfinity
      val particleCosts = new Array[Double](totalParticleCentroids.length)
      for(i <- 0 until totalParticleCentroids.length){
        val bcParticle = sc.broadcast(totalParticleCentroids(i))
        val totalCost = data.mapPartitions{ points =>
          points.map { point =>
            val centroids = bcParticle.value
            val (bestCenter, cost) = ClusteringUtils.findClosest(centroids, point)
            cost
          }
        }.reduce{case (a,b)=>a+b}


        bcParticle.destroy()
        val averageCost = totalCost/dataSize
        particleCosts(i) = averageCost
        if(bestCost >= averageCost){
          bestCost = averageCost
          bestIndex = i
        }
      }

      val optionCentroid: Option[Array[VectorWithNorm]] = totalParticleCentroids.lift(bestIndex)
      if(optionCentroid == None){
        throw new Exception(" newBestGlobalCentroids is null!!!")
      }
      val newBestGlobalCentroids = optionCentroid.get
      if(bestGlobalCentroids != null){
        converged = true
        for(i <- 0 until newBestGlobalCentroids.length){
          if(converged && ClusteringUtils.fastSquaredDistance(newBestGlobalCentroids(i),bestGlobalCentroids(i))>  epsilon * epsilon){
            converged = false
          }
        }
      }

      bestGlobalCentroids = newBestGlobalCentroids
      iteration += 1

      if(bestCost < resultCost){
        resultBestGlobalCentroids = bestGlobalCentroids
        resultCost = bestCost
      }

      //update the swarm in a parallel way.
      if(!converged && iteration < maxIterations){
        val currentInertiaWeight:Double =
          maxInertiaWeight-((maxInertiaWeight - minInertiaWeight)*iteration/maxIterations)
        val oldParalellizedSwarm = paralellizedSwarm
        paralellizedSwarm = oldParalellizedSwarm
          .map{particle=>
            particle.updateLocalBest(particleCosts(particle.id))
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

  override def run(//?? I didn't use Instrumentation because I don't know what it is lol
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

}

object PSOClustering2{
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
    new PSOClustering2().setK(k)
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
    new PSOClustering2().setK(k)
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
    new PSOClustering2().setK(k)
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
    new PSOClustering2().setK(k)
      .setMaxIterations(maxIterations)
      .setSwarmSize(swarmSize)
      .setInitialModel(model)
      .setCentroidMatching(centroidMatching)
      .run(data)
  }

}