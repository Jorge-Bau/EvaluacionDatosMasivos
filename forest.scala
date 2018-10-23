import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

// se carga y se analizan los datos y se convierte en un DataFrame
val data = MLUtils.loadLibSVMFile(sc, "data.txt")
// // Se dividen los datos en conjuntos de prueba y entrenamiento(el 30% se agoto para la prueba)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1)) //

// Se entrena el modelo de Bosque Aleatorio
// caracter. vacias //categoricalFeaturesInfo indica que todas las categorias son continuas
val numClasses = 2 // se le asigna el numero de clases
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 4 // se le asigna el numero de arboles
val featureSubsetStrategy = "auto" // Deja que el algoritmo elija
val impurity = "gini" // basura
val maxDepth = 4 // profundidad
val maxBins = 32 // contenedores
// entrada del modelo se le pasan los parametros
val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// se evalua el modelo en las instancias de prueba y calcula el error de prediccion
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

// filtra el resultado con mayor numero de votos para no mostrar todos los datos.
val testEr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println(s"Test Error = $testErr")
println(s"Learned classification forest model:\n ${model.toDebugString}")

// Guarda y carga el modelo
model.save(sc, "target/tmp/myRandomForestClassificationModell4")
val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModell4")
