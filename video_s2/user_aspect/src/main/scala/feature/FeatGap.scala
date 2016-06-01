package feature

class FeatGap extends Feature {
  var feats: Map[String, Map[Int, Int]]= null

  def featGen( data: Array[(String, Int, Int, Int, Int)] ,week_id:Int): Unit ={
    feats =  data.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 ).map {
      group =>
        val (uid, vid) = group._1
        val last_day = group._2.map(y => y._2._1).sorted.max
        val day = (week_id - 1) * 7 + 1
        val day_gap = day - last_day
        ( uid, vid, day_gap )
    }.groupBy( _._1 ).map{
      group =>
        val uid = group._1
        val values = toMap[Int](group._2)
        (uid, values)
    }
  }

  def getFeat(uid:String,vid:Int): String ={
    if( !feats.contains(uid)|| !feats(uid).contains(vid) ) s"\t0"
    else {
      val day_gap = feats(uid)(vid)
      s"\t${day_gap}"
    }
  }

}
