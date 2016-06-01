package feature

import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Administrator on 2016/3/13.
  */
class FeatUser(data:RDD[(String, Int, Int, Int, Int,String, Int)])  extends Serializable{

  val feats: Map[String, (Int, Int)] = data.map {
    case (uid,week,day,hh,mm,labels,cnt) =>
      (uid,(1,cnt) )
  }.reduceByKey{
    case (x,y) =>
      ( x._1 + y._1 ,
        x._2 + y._2)
  }.collectAsMap()

  def getFeat(uid:String): String ={
    if( !feats.contains(uid)) s",0,0"
    else{
      val (cnt,sum) = feats(uid)
      s",$cnt,$sum"
    }
  }
}
