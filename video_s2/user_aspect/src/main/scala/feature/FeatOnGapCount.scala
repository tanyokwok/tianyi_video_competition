package feature

import org.saddle.Vec

/**
  * Created by Administrator on 2016/1/16.
  */
class FeatOnGapCount extends Feature {
  def caculate(group: ((String, Int), Array[((String, Int), (Int, Int))]),
               week_id: Int) = {
    val (uid, vid) = group._1
    val visit_days = group._2.map(y => y._2._1).sorted
    val day = (week_id - 1) * 7 + 1
    val left = Vec((visit_days.toList :+ day).toArray)
    val right = Vec((0 :: visit_days.toList).toArray)

    val avg_gap = (left - right).mean

    val last_day = group._2.map(y => y._2._1).sorted.max
    (uid, vid, (avg_gap, last_day))
  }

  var feats: Map[String, Map[Int, (Double, Int)]] = null

  def featGen(data: Array[(String, Int, Int, Int, Int)], week_id: Int): Unit = {
    val candidate = data.map {
      case (uid, week, day, video_site, watch_count) => ((uid, video_site), ((week - 1) * 7 + day, watch_count))
    }

    feats = candidate.groupBy(_._1).map { group =>
      //_._1 =>表示候选的标识， _._2 =>( (uid, vid), (day, wcount) )
      caculate(group, week_id)
    }.groupBy(_._1).map {
      group =>
        val uid = group._1
        val values = toMap[(Double, Int)](group._2)
        (uid, values)
    }

  }

  def getFeat(uid: String, vid: Int, week_id:Int): String = {
    if (!feats.contains(uid) || !feats(uid).contains(vid)) s"\t0"
    else {
      val (avg_gap, last_day) = feats(uid)(vid)
      val feat = Range(0, 7).map {
        did =>
          val day = (week_id - 1) * 7 + did + 1
          val day_gap = day - last_day
          //如果差异小于1,则表示gap落在在当天
          if ((day_gap - avg_gap * (day_gap / avg_gap).toInt) < 1)
            1
          else 0
      }.sum
      s"\t${feat}"
    }

  }
}
