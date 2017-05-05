/**
  * Created by work on 26/04/2017.
  */

package object domain {

  // for batch jobs
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )

  case class ActivityByProduct (product : String,
                                timestamp_hour : Long,
                                purchase_count : Long,
                                add_to_cart_count : Long,
                                page_view_count : Long)

  // for mlib
  case class Movie(movieId: Int,
                   title:String)

  case class User(userId: Int,
                  gender: String,
                  age : Int,
                  occupation: Int,
                  zip: String)

}
