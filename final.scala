import org.apache.spark.sql.Encoders //sirve para poder utilizar el codigo de schema y colocar el nombre al encabezado
import org.apache.spark.sql.{DataFrame, SparkSession} //sirve para la conexion de los daots
import org.apache.spark.{SparkConf, SparkContext} //sirve para la conexion de los datos
import org.apache.spark.ml.feature.VectorAssembler //para crear los vectores de ml
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)


case class Initial(sepalLength: Option[Double], sepalWidth: Option[Double], petalLength: Option[Double], petalWidth: Option[Double], species: Option[String])
case class Final(sepalLength : Double, sepalWidth : Double, petalLength : Double, petalWidth : Double, species: Double)

//se crea el inicio de sesion
val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
val sparkSession = SparkSession.builder.config(conf = conf).appName("spark session example").getOrCreate()
val path = "Iris.csv"
//se coloca nombre al encabezado de la tabla para que sea mas facil su modificacion
var irisSchema2 = Encoders.product[Initial].schema
val iris: DataFrame = sparkSession.read.option("header","true").option("inferSchema", "true").schema(irisSchema2).csv(path)
iris.show()
val assembler = new VectorAssembler().setInputCols(Array("sepalLength", "sepalWidth", "petalLength", "petalWidth", "species")).setOutputCol("features")

def autobot(in: Initial) = Final(
    in.sepalLength.map(_.toDouble).getOrElse(0),
    in.sepalWidth.map(_.toDouble).getOrElse(0),
    in.petalLength.map(_.toDouble).getOrElse(0),
    in.petalWidth.map(_.toDouble).getOrElse(0),
    in.species match {
      case Some("Iris-versicolor") => 1;
      case Some("Iris-virginica") => 2;
      case Some("Iris-setosa") => 3;
      case _ => 3;
    }
  )

val data = assembler.transform(iris.as[Initial].map(autobot))
data.show()


import org.apache.spark.ml.clustering.KMeans

//trains the k-means model
val kmeans = new KMeans().setK(3).setSeed(1L)
val model = kmeans.fit(data)

// Evaluate clustering by calculate Within Set Sum of Squared Errors.
val WSSE = model.computeCost(data)
println(s"Within set sum of Squared Errors = $WSSE")

// Show results
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Loads and parses data
def parse(line: String): Vector = Vectors.dense(line.split(" ").map(_.toDouble))

// Clustering the data into 6 clusters by BisectingKMeans.
val bkm = new BisectingKMeans().setK(6)
val model = bkm.run(data)

// Show the compute cost and the cluster centers
model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
  println(s"Cluster Center ${idx}: ${center}")
}
