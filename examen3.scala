import org.apache.spark.sql.SparkSession

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

import org.apache.spark.ml.clustering.KMeans

val data  = spark.read.option("header","true").option("inferSchema", "true").format("csv").load("Wholesale_customers_data.csv")

import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.linalg.Vectors

val feature_data = data.drop("Channel", "Region")

//Transform to vector to the ml algorith can read the input
val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")).setOutputCol("features")
val dt = assembler.transform(feature_data)


//trains the k-means model
val kmeans = new KMeans().setK(3).setSeed(1L)
val model = kmeans.fit(dt)

// Evaluate clustering by calculate Within Set Sum of Squared Errors.
val WSSE = model.computeCost(dt)
println(s"Within set sum of Squared Errors = $WSSE")

// Show results
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
