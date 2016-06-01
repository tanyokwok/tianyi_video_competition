package feature

import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Administrator on 2016/3/13.
  */
class FeatUser(data:RDD[(String, Int, Int, Int, Int,String, Int)])  extends Serializable{

  val feats: Map[String, (Int, Int)] = data.map {
    case (uid,week,day,hh,mm,labels,cnt) =>
      (uid,week,day,cnt)
  }.groupBy(_._1).map {
    group =>
      val uid: String = group._1
      val grp2: Iterable[(String, Int, Int, Int)]
      = group._2

      val cnt = grp2.size
      val sum = grp2.map {
        case (uid, week, day, cnt) =>
          cnt
      }.sum
      (uid,(cnt,sum))
  }.collectAsMap()

  def getFeat(uid:String): Unit ={
    if( !feats.contains(uid)) s"\t0\t0"
    else{
      val (cnt,sum) = feats(uid)
      s"\t$cnt\t$sum"
    }
  }
}
