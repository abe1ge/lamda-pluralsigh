package mlLib

import domain.{Movie, User}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.spark.mllib.recommendation.{ALS, M}
/**
  * Created by Administrator on 04/05/2017.
  */
object MlMovie {
  def main(args: Array[String]): Unit = {

    // setup spark context
    //setup spark context
    val conf = new SparkConf().setAppName("Streamer").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val userSF = "sparklambda/src/main/resources/users.dat"
    val moviesSF = "sparklambda/src/main/resources/movies.dat"
    val ratingSF = "sparklambda/src/main/resources/ratings.dat"

    print("Printing data \n")
    userSF.take(5)
    val userData = sc.textFile(userSF).map(x => x.split("::"))
    val moviesData = sc.textFile(moviesSF).map(x => x.split("::"))
    val ratingData = sc.textFile(ratingSF).map(x => x.split("::"))

    val userRDD = userData.map(x => User(x(0).toInt, x(1).toString, x(2).toInt, x(3).toInt, x(4).toString))
    val movieRDD = moviesData.map(x => Movie(x(0).toInt, x(1)))
    val ratingRDD = ratingData.map(x => Rating(x(0).toInt, x(1).toInt, x(2).toFloat))

    print("Printing data \n")
    userRDD.first()
    movieRDD.take(10)
    ratingRDD.take(10)

  }

}
