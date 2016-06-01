package feature

/**
  * Created by Administrator on 2016/3/19.
  */
class FeatByHour(data: Array[(String, Int, Int, Int, Int, Int, Int)], week_id:Int, interval:Int) {

  //每天有多少个钟头点击了网站
  val hour_by_day = data.filter{
    case ( uid, week, day, vedio_site, click_count, hh, mm)=>
      week >= (week_id-interval)
  }.map {
    case (uid, week, day, vid, cnt, hh, mm) =>
      ((uid, vid, day), 1)
  }.groupBy(_._1).map{
    case (key,group) =>
      val freq = group.size
      (key, freq)
  }

  val feats: Map[(String, Int), (Int, Double, Int)] = hour_by_day.map{
    case ( (uid,vid,day), freq) =>
      ( (uid,vid), freq)
  }.groupBy( _._1 ).map{
    case (key, group ) =>
      val data = group.map{
        case (k, freq)=> freq
      }
      ( key,( data.max, data.sum/data.size.toDouble, data.sum))
  }

  val userfeats = hour_by_day.map{
    case ( (uid,vid,day), freq) =>
      ( (uid,day), freq )
  }.groupBy( _._1 ).map{//汇总同一天，不同视频网站的时间频率
    case ((uid,day),group ) =>
      val sum = group.map{
        case (k,freq) => freq
      }.sum
      (uid,sum)//将所有网站的频率加和
  }.groupBy(_._1).map{
    case (uid,group)=>
       val data = group.map{
        case (k, freq)=> freq
      }
      //一周中所有天的最大值，平均值，总和
      ( uid,( data.max, data.sum/data.size.toDouble, data.sum))
  }

  def getFeats( uid:String , vid :Int):String={
    if( feats.contains((uid,vid))){
      val (f1,f2,f3) = feats( (uid,vid))
      s"\t$f1\t$f2\t$f3"
    }else{
      s"\t0\t0\t0"
    }
  }

  def getUserFeats( uid:String):String={
    if( userfeats.contains( uid )){
      val (f1,f2,f3)=userfeats(uid)
       s"\t$f1\t$f2\t$f3"
    }else{
      s"\t0\t0\t0"
    }
  }
}
