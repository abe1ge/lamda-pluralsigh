package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by work on 26/04/2017.
  */
object BatchJob {
  def main(args: Array[String]): Unit = {

    // get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    // check if running from IDE
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "C:\\winutils") // required for winutils
      conf.setMaster("local[*]")
    }

    // setup spark
    val sc = new SparkContext(conf)

    // initialize input RDD
    val sourceFile = "file:///C:/Users/work/Documents/plursight/applyingLambdaArchitecture/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    // Spark action results in job execution
    input.foreach(println)
  }
}
