package analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scopt.OptionParser

import scala.collection.Map

/**
  * Created by Administrator on 2016/3/15.
  */
object LabelAnalysis{

  def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("TFIDF")
      .set("spark.hadoop.validateOutputSpces","false")

    if( p.islocal )
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val join_RDD: RDD[(String, ((String, Int), (String, Int, Int)))]
    = loadJoin(sc, p.join_pt )

    val stat = join_RDD.map{
      case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
      (labels,1)
    }.reduceByKey( _ + _ )

    val label_sorted = stat.sortBy(_._2).map{
      case (label,cnt)=>
      s"$label\t$cnt"
    }

    label_sorted.saveAsTextFile(p.label_pt)
  }

  def loadJoin(sc:SparkContext, join_pt:String):
  RDD[(String, ((String, Int), (String, Int, Int)))] ={
    val join_data:RDD[(String, ((String, Int), (String, Int, Int)))]
    = sc.objectFile(join_pt)
    join_data
  }
  case class Params(join_pt:String = "",
                    label_pt:String = "",
                    threshold:Int = 0,
                    islocal:Boolean=false)

  val parser = new OptionParser[Params]("Analysis"){
    opt[String]("join_pt")
      .text("join文件路径")
      .action{ (x,c) => c.copy( join_pt = x) }
    opt[String]("label_pt")
      .text("label文件输出路径")
      .action{ (x,c) => c.copy( label_pt = x) }
    opt[Boolean]("islocal")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( islocal = x) }
  }

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
