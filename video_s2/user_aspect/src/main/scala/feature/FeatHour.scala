package feature

/**
  * 计算用户历史中在每个小时点击的累计次数，累加和
  */
class FeatHour {

  var feats : Map[(String, Int), (Int, Int)]= null
  def featGen(data: Array[(String, Int, Int, Int, Int, Int, Int)] ): Unit ={
    feats = data.map{
      case ( uid, week, day, vedio_site, click_count, hh, mm)=>
        ((uid,hh),(1,click_count))
    }.groupBy( _._1 ).map{
      case group =>
        val (uid,hh) = group._1
        val (cnt_sum, click_sum) = group._2.map{
          case ((uid,hh),(cnt,click_cnt))=>
            (cnt,click_cnt)
        }.reduce{ (x,y) => ( x._1 + y._1, x._2 + y._2)}
        ((uid,hh),(cnt_sum,click_sum))
    }
  }

  def getFeat(uid:String,hh:Int): String={
    if( feats.contains( (uid,hh))){
      val (f1,f2) = feats( (uid,hh))
      return s"\t$f1\t$f2"
    }else{
      return s"\t0\t0"
    }
  }
}
