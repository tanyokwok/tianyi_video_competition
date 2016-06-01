package feature

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/14.
 */
class FeatStatDecay extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), (Int, Int))])) = {
    val ( uid, vid) = group._1
    val watch_counts = scala.collection.mutable.MutableList[Int]()
    group._2.foreach{
      case ((uid, vid), (week, count)) =>
        for( i <- 0 until math.pow(2,week).toInt ){
          watch_counts+= count
        }
    }
    //      watch_counts.foreach(println)
    val vec = Vec(  watch_counts.toArray )
    val mean = vec.mean
    val median = vec.median
    val stdev = vec.stdev
    (uid,vid,(mean,median,stdev))
  }


  var feats: Map[String, Map[Int, (Double, Double, Double)]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)] ,begin_week:Int): Unit ={
    feats =  data.map{
      case ( uid, week, day, video_site, watch_count )
      =>  ( (uid , video_site) ,( week - begin_week, watch_count) )
    }.groupBy( _._1 )
      .map{
        group => calculate(group)
      }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[(Double, Double,Double)](group._2)
        (uid,values)
    }
  }


  def getFeat(uid:String,vid:Int): String ={

    if( !feats.contains(uid)|| !feats(uid).contains(vid) ) s"\t0\t0\t0"
    else{
      val (mean,median,stdev) = feats(uid)(vid)
      s"\t$mean\t$median\t$stdev"
    }
  }
}
