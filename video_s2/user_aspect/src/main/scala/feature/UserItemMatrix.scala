package feature

import org.saddle.Vec

/**
 * 计算用户<uid,vid>对的历史点击次数
 */
class UserItemMatrix extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), Int )])) = {
    val ( uid, vid) = group._1
    val watch_counts: Int = group._2.map(y => y._2 ).sum

    (uid,vid,watch_counts)
  }

  var feats: Map[String, Map[Int,Int ]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)]): Unit ={
    feats =  data.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) , watch_count )
    }.groupBy( _._1 )
      .map{
        group => calculate(group)
      }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[Int](group._2)
        (uid,values)
    }
  }

  def getFeat(uid:String,vid:Int): String ={

    if( !feats.contains(uid)|| !feats(uid).contains(vid) ) s"\t0"
    else{
      val t: Int = feats(uid)(vid)
      s"\t$t"
    }
  }
}
