package wikipedia

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "src/main/resources/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("SimpleApp").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    val minPartitions = 2
    val logData = sc.textFile(logFile, minPartitions).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
