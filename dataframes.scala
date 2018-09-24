import org.apache.spark.sql.SparkSession
val spar = SparkSession.builder().getOrCreate()

//1
val df = spark.read.option("header", "true").option("inferSchema","true")csv("Netflix_2011_2016.csv")

//3
df.columns.sorted

//4
df.printSchema()


//5
df.select($"Date", $"Open", $"High", $"Low", $"Close").show()

//6
df.describe().show()

//7
val df7 = df.withColumn("HV Ratio",df("High")/df("Volume"))
df7.show()

//8
val df8 = df.withColumn("Day", dayofyear(df("Date")))
val d81 = df8.drop("Date","Open","Low","Close","Volume","Adj Close")
d81.createOrReplaceTempView("Next")
val sqlDF = spark.sql("SELECT MAX(High)FROM Next  ")
val sqlDF = spark.sql("SELECT * FROM Next WHERE High > 715 ")
sqlDF.show()

//9
// Toma el valor con el cual cierra su valor en el mercado en el final del dia.

//10
val Volumen = df.columns(Volume)
df.agg(min(Volumen), max(Volumen))


//11
//a)
df.filter($"Close" < 600).show()

//b
(df.filter($"High" > 500).count()*1.0 / df.count()) * 100

//c
df.select(corr("High","Low")).show()

//d
val df2 = df.withColumn("Year", year(df("Date")))
val dfmax = df2.groupBy("Year").max()
dfmax.select($"Year", $"Max(High)").show()

//e)
df.select(year(df("Date"))).show()

val df2 = df.withColumn("Year", year(df("Date")))

val dfavgs = df2.groupBy("Year").mean()

dfavgs.select($"Year", $"avg(Close)").show()
