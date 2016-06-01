package feature

import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * 计算interval周内每天在线的钟头数，求最大值，最小值，平均值
  */
class FeatActionHour(data:RDD[(String, Int, Int, Int, Int,String, Int)],week_id:Int, interval:Int)  extends Serializable{

  val every_day_count: Array[((String, Int, Int), Int)] = data.filter{
    case (uid,week,day,hh,mm,labels,cnt) =>
      week >= ( week_id - interval)
  }.map{
    case (uid,week,day,hh,mm,labels,cnt) =>
      ((uid,week,day,hh),1)
  }.reduceByKey( _ + _).map {
    case ((uid,week,day,hh),cnt) =>
      ( (uid,week,day),1 )
  }.reduceByKey(_ +_).collect()

  val feats = every_day_count.map{
    case ((uid,week,day),cnt)=>
      (uid,cnt)
  }.groupBy(_._1).map{
    case (uid, group)=>
      val grp = group.map{
        case (k,cnt)=>
          cnt
      }
      (uid, ( grp.sum/grp.size.toDouble, grp.max, grp.min  ))
  }

  def getFeat(uid:String): String ={
    if( !feats.contains(uid)) s"\t0\t0\t0"
    else{
      val (mean,max,min) = feats(uid)
      s"\t$mean\t$max\t$min"
    }
  }
}
