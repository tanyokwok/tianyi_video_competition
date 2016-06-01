package feature

import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
  * Created by Administrator on 2016/3/15.
  */
class FeatProb(join_RDD:RDD[(String, ((String, Int), (String, Int, Int)))],
               probVgivenLRDD: RDD[((String, Int), (Double, Double, Double))]) extends Serializable{

  val probVgivenL: Map[(String, Int), (Double, Double, Double)] = probVgivenLRDD.collectAsMap()
  val feats: Map[(String, Int), (Double, Double, Double)] = join_RDD.map{
    case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
      if( probVgivenL.contains((labels,vid))){
        val prob: (Double, Double, Double) = probVgivenL( (labels,vid) )
        ((uid,vid),(prob._1,prob._2,prob._3))
      }
      else ( (uid, vid), (0.0,0.0,0.0))
  }.reduceByKey{
    case (x, y) =>
      ( x._1 + y._1,
        x._2 + y._2,
        x._3 + y._3)
  }.collectAsMap()

  def getFeat(uid:String, vid:Int): String ={
    if( !feats.contains( (uid,vid)) ) s",0,0,0"
    else{
      val (p1,p2,p3) = feats((uid,vid))
      s",$p1,$p2,$p3"
    }
  }

}
