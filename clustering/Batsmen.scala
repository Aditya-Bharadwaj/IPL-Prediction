import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._
 
val data = sc.textFile("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/1. StepWise/Step2/AB/BattingStats.csv")
val header = data.first
val rows = data.filter(l => l != header)

case class CC1(Pos:Int, Player:String, Mat:Int, Inns:Int, NO:Int, Runs:Long, HS:Int, Avg:Double, BF:Long, SR:Double, Hundreds:Int, Fifties:Int, Fours:Int, Sixes:Int)

val allSplit = rows.map(line => line.split(","))

val allData = allSplit.map(p => CC1(p(0).toInt, p(1).toString, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toLong, p(6).trim.toInt, p(7).trim.toDouble, p(8).trim.toLong, p(9).trim.toDouble, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt))

val allDF = allData.toDF()
val rowsRDD = allDF.rdd.map(r => (r.getInt(0), r.getString(1), r.getInt(2), r.getInt(3), r.getInt(4), r.getLong(5), r.getInt(6), r.getDouble(7), r.getLong(8), r.getDouble(9), r.getInt(10), r.getInt(11), r.getInt(12), r.getInt(13) ))

rowsRDD.cache()
val vectors = allDF.rdd.map(r => Vectors.dense(r.getInt(4), r.getLong(5), r.getLong(8), r.getDouble(9), r.getInt(12), r.getInt(13)))

vectors.cache()
val kMeansModel = KMeans.train(vectors, 10, 998)

kMeansModel.clusterCenters.foreach(println)

val predictions = rowsRDD.map{r => (r._2, kMeansModel.predict(Vectors.dense(r._5, r._6, r._9, r._10, r._13, r._14)))}
val predDF = predictions.toDF("Player", "CLUSTER")
val t = allDF.join(predDF, "Player")

t.filter("CLUSTER=0").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster0")
t.filter("CLUSTER=1").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster1")
t.filter("CLUSTER=2").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster2")
t.filter("CLUSTER=3").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster3")
t.filter("CLUSTER=4").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster4")
t.filter("CLUSTER=5").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster5")
t.filter("CLUSTER=6").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster6")
t.filter("CLUSTER=7").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster7")
t.filter("CLUSTER=8").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster8")
t.filter("CLUSTER=9").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Batting2/cluster9")

