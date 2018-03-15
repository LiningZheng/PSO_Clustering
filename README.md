# PSO_Clustering
This scalable PSO-based (particle swarm optimization) clustering algorithm is implemented with Apache Spark. I refered to the K-Means implementation in Spark MLlib [1] and this paper [2]. PSOClustering is consistent with the approach proposed in [2]. However, PSOClustering2 evaluates individual particle solutions seperately and sequentially in an iterative way instead of creating keys like (Particle ID, centroid ID).


[1] https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/clustering/KMeans.scala
[2] Aljarah, Ibrahim, and Simone A. Ludwig. "Parallel particle swarm optimization clustering algorithm based on mapreduce methodology." Nature and biologically inspired computing (NaBIC), 2012 fourth world congress on. IEEE, 2012

Please Feel free to let me know any bugs or suggestions to improve the algorithm. Email: lzhen039@uottawa.ca
