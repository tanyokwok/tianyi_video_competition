package feature

import org.saddle.Vec

/**
 * 计算用户<uid,vid>对，历史的访问情况的mean，median,stdev
 */
class FeatStat extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), (Int, Int))])) = {
    val ( uid, vid) = group._1
    val watch_counts = group._2.map( y => y._2._2 ).toArray

    val vec = Vec(  watch_counts )
    val mean = vec.mean
    val median: Double = vec.median
    val stdev = vec.stdev
    (uid,vid,(mean,median,stdev))
  }

  var feats: Map[String, Map[Int, (Double, Double, Double)]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)]): Unit ={
    feats =  data.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
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
