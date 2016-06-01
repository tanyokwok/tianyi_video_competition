package feature

import data.extract.DataLoad
import org.saddle.Vec

/**
  * 统计每个<uid,vid>平均访问的时间隔
  */
class FeatAvgGap extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), (Int, Int))]),
                week_id:Int ):(String, Int, Double) ={
    val ( uid, vid) = group._1
    val visit_days = group._2.map( y => y._2._1 ).sorted

    val day = ( week_id - 1 ) * 7 + 1
    val left = Vec(  ( visit_days.toList:+ day ).toArray )
    val right = Vec( ( 0::visit_days.toList).toArray )

    val avg_gap = (left - right ).mean

    (uid, vid, avg_gap)

  }
  var feats: Map[String, Map[Int, Double]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)] ,week_id:Int): Unit ={
     feats =  data.map{
       case ( uid, week, day, video_site, watch_count )
       => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
     }.groupBy( _._1 )
      .map{
        group => calculate(group,week_id)
      }.groupBy( _._1 ).map{
      group =>
        val uid = group._1
        val values = toMap[Double](group._2)
        (uid, values)
    }
  }

  def getFeat(uid:String,vid:Int): String ={

    if( !feats.contains(uid)|| !feats(uid).contains(vid) ) s"\t0"
    else{
      val avg_gap: Double = feats(uid)(vid)
      s"\t$avg_gap"
    }
  }

}
