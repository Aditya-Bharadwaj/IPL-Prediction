import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._

/*
Pos,Player,Mat,Inns,Overs,Runs,Wkts,BBI,Avg,Econ,SR,FourW,FiveW
Required: Inns(3), Runs(5),Wkts(6), Avg(8), Economy(9), SR(10), FourW(11)
Pos(0) Integer
Player(1) String
Mat(2) Integer
Inns(3) Integer 
Overs(4) Double <-
Runs(5) Integer <-
Wkts(6) Integer <-
BBI(7) String
Avg(8) Double 
Econ(9) Double <-
SR(10) Double <-
FourW(11) Integer <-
FiveW(12) Integer
*/

val data = sc.textFile("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/1. StepWise/Step2/AB/BowlingStats.csv")

val header = data.first
val rows = data.filter(line => line != header)
case class columns(Pos: Integer, Player: String, Mat: Integer, Inns: Integer, Overs: Double, Runs: Integer, Wkts: Integer, BBI: String, Avg: Double, Econ: Double, SR: Double, FourW: Integer, FiveW: Integer)
val allSplit = rows.map(line => line.split(','))
val allData = allSplit.map(part => columns(part(0).trim.toInt,part(1).trim.toString,part(2).trim.toInt,part(3).trim.toInt,part(4).trim.toDouble, part(5).trim.toInt, part(6).trim.toInt, part(7).trim.toString,part(8).trim.toDouble, part(9).trim.toDouble, part(10).trim.toDouble, part(11).trim.toInt,part(12).trim.toInt))
val allDF = allData.toDF()
val rowsRDD = allDF.rdd.map(r => (r.getInt(0),r.getString(1),r.getInt(2),r.getInt(3),r.getDouble(4), r.getInt(5), r.getInt(6), r.getString(7),r.getDouble(8), r.getDouble(9), r.getDouble(10), r.getInt(11), r.getInt(12)))
rowsRDD.cache()
val vectors = allDF.rdd.map(r => Vectors.dense(r.getDouble(4), r.getInt(6), r.getDouble(9), r.getDouble(10), r.getInt(11)))
vectors.cache()
val kMeansModel = KMeans.train(vectors, 10, 998)
kMeansModel.clusterCenters.foreach(println)
val predictions = rowsRDD.map{r => (r._2, kMeansModel.predict(Vectors.dense(r._5, r._7, r._10, r._11, r._12)))}
val predDF = predictions.toDF("Player", "CLUSTER")
val t = allDF.join(predDF, "Player")

t.filter("CLUSTER=0").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster0")
t.filter("CLUSTER=1").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster1")
t.filter("CLUSTER=2").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster2")
t.filter("CLUSTER=3").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster3")
t.filter("CLUSTER=4").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster4")
t.filter("CLUSTER=5").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster5")
t.filter("CLUSTER=6").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster6")
t.filter("CLUSTER=7").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster7")
t.filter("CLUSTER=8").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster8")
t.filter("CLUSTER=9").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/sb/Documents/Big Data/IPL Analysis/1. StepWise/Step2/AB/Clusters/Bowling1/cluster9")
