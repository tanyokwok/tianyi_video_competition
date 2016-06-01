package feature

/**
 * 计算用户<uid,vid>对的历史点击次数
 */
class UserWeekMatrix extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), Int )])) = {
    val ( uid, week) = group._1
    val watch_counts: Int = group._2.map(y => y._2 ).sum

    (uid,week,watch_counts)
  }

  var feats: Map[String, Map[Int,Int ]] = null

  def featGen( data: Array[(String, Int, Int, Int, Int)], week_id:Int): Unit ={
    feats =  data.filter{
      case ( uid, week, day, video_site, watch_count )=>
        week >= week_id - 5
    }.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , week) , watch_count )
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

  def getFeat(uid:String,week:Int): String ={

    if( !feats.contains(uid)|| !feats(uid).contains(week) ) s"\t0"
    else{
      val t: Int = feats(uid)(week)
      s"\t$t"
    }
  }
}
