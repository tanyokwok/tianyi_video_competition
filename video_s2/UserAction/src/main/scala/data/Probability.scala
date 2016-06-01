package data

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.Map

/**
  * 用户对每个网站的tf-idf权重
  */
object Probability extends Serializable{

  def createProb(sc:SparkContext,
                 join_pt:String,
                 prob_pt:String,
                 threshold: Int) {
    val join_result:RDD[(String, ((String, Int), (String, Int, Int)))]
    = sc.objectFile(join_pt)

    val data = join_result.map {
      case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
        ((labels, vid), (1, lcnt, vcnt))
    }

    //tfs中的key都是唯一的了
    val numLV: RDD[((String, Int), (Int, Int, Int))]
    = data.reduceByKey {
      case ((v1_1, v1_2, v1_3), (v2_1, v2_2, v2_3)) =>
        (v1_1 + v2_1, v1_2 + v2_2, v1_3 + v2_3)
    }

    val rdd_numL = numLV.map {
      case ((labels, vid), (cnt, lsum, vsum)) =>
        (labels, (cnt, lsum, vsum))
    }.reduceByKey {
      case (x, y) =>
        ( x._1 + y._1,
          x._2 + y._2,
          x._3 + y._3 )
    }

    val numL = rdd_numL.collectAsMap()

    val probVgivenL: RDD[((String, Int), (Double, Double, Double))]
    = numLV.filter{
      case ((labels, vid), (cnt, lcnt, vcnt)) =>
        //过滤掉记录数小等于1000的Label
        numL(labels)._1 > threshold
    }.map{
      case ((labels, vid), (cnt, lcnt, vcnt)) =>
        println( s"$labels,$vid\t$cnt/${numL(labels)._1}," +
          s"$lcnt/${numL(labels)._2}," +
          s"$vcnt/${numL(labels)._3}")
        ((labels, vid),( cnt.toDouble/ numL(labels)._1,
          lcnt.toDouble/ numL(labels)._2,
          vcnt.toDouble/ numL(labels)._3))
    }

    val filter_numL = rdd_numL.filter{
      case (labels, (cnt, lcnt, vcnt)) =>
        //过滤掉记录数小等于1000的Label
        numL(labels)._1 > threshold
    }
    val totalNum: (Int, Int, Int) = filter_numL.map{ case (x,y)=> y}.reduce{
      case (x, y) =>
        ( x._1 + y._1,
          x._2 + y._2,
          x._3 + y._3 )
    }

    val probL = filter_numL.map{
      case (labels,(cnt,lsum,vsum)) =>
        (labels,
          ( cnt.toDouble/ totalNum._1,
            lsum.toDouble/ totalNum._2,
            vsum.toDouble/ totalNum._3)
          )
    }

    probL.saveAsObjectFile( prob_pt + "_probL")
    probVgivenL.saveAsObjectFile( prob_pt )
    probVgivenL.map{
      case ((labels, vid), (f1,f2,f3)) =>
        val line = new StringBuffer(
          s"$labels\t$vid\t$f1\t$f2\t$f3")
        line.toString
    }.saveAsTextFile(s"${prob_pt}_text")
  }
//  //(key,((labels,lcnt),(uid,vid,vcnt)))


  def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("TFIDF")
      .set("spark.hadoop.validateOutputSpces","false")

    if( p.islocal )
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    createProb(sc,p.join_pt,p.prob_pt,p.threshold)
  }

  val parser = new OptionParser[Params]("TFIDF"){
    opt[String]("join_pt")
      .text("join文件路径")
      .action{ (x,c) => c.copy( join_pt = x) }
    opt[String]("prob_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( prob_pt = x) }
    opt[Int]("threshold")
      .text("过滤Label的阈值，出现次数小于threshold的会被过滤")
      .action{ (x,c) => c.copy( threshold = x)}
        opt[Boolean]("islocal")
      .text("是否是单机")
      .action{ (x,c) => c.copy( islocal = x) }
  }

  def loadTFIDF(sc:SparkContext,tfidf_pt:String ):
  RDD[(String, Predef.Map[Int, (Double, Double, Double)])] ={
    val tfidf_pairs:RDD[(String, Predef.Map[Int, (Double, Double, Double)])]
    = sc.objectFile(tfidf_pt)
    tfidf_pairs
  }

  def loadJoin(sc:SparkContext, join_pt:String):
  RDD[(String, ((String, Int), (String, Int, Int)))] ={
    val join_data:RDD[(String, ((String, Int), (String, Int, Int)))]
    = sc.objectFile(join_pt)
    join_data
  }
  case class Params(join_pt:String = "",
                    prob_pt:String = "",
                    threshold:Int = 0,
                    islocal:Boolean=false)

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
