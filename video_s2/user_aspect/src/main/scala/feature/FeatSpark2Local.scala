package feature

import scala.io.Source

/**
  * Created by Administrator on 2016/3/20.
  */
class FeatSpark2Local( spark_feat_pt:String ) {

  val (feat_num, sparkFeats) = loadSparkFeat()
  def loadSparkFeat():
  (Int, Map[String, String])={
    var feat_num = 0
    val res = Source.fromFile( spark_feat_pt).getLines().map{
      line =>
        val idx = line.indexOf('\t')
        val uid = line.substring(0,idx)
        val feat_str = line.substring(idx+1).replace(",","\t")
        if( feat_num == 0 ){
          feat_num = feat_str.trim.split("\t").size
        }else{
          if( feat_num != feat_str.trim.split("\t").size){
            println( "feat_num is not consistant at: " + feat_str)
            System.exit(-1)
          }
        }
        (uid, feat_str)
    }.toMap
    (feat_num, res)
  }


  def getFeats( uid:String ):String ={
        if( sparkFeats.contains(uid)){
          s"\t${sparkFeats(uid)}"
        }
        else{
          val feat_line = new StringBuffer()
          println( s"$uid not hit")
          for( i <- 0 until feat_num){
            feat_line.append( s"\t0")
          }
          feat_line.toString
        }

  }
}
