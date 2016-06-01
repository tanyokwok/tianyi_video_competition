package feature

import data.BehaviorExtract
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.Map

/**
  * Created by Administrator on 2016/3/17.
  */
object GenUserActionMatrix extends serializable{
 def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("UserLabelMatrix")
      .set("spark.hadoop.validateOutputSpces","false")

    val sc = new SparkContext(conf)

    val begin_week = p.week_id - p.interval
    val end_week = p.week_id

    val data = BehaviorExtract.load( sc, p.behavior_pt)
    val behavior
    = data.filter{
      case (uid, week, day, hh, mm, labels, cnt) =>
        week >= ( end_week - 5 )
    }.map{
      case (uid, week, day, hh, mm, labels, cnt) =>
        ((uid,week), cnt)
    }.reduceByKey(_ + _)

  val feat_result: RDD[String] = behavior.map{
     case ((uid,week), cnt) =>
       (uid,Map( (week,cnt) ))
   }.reduceByKey( _ ++ _ ).map{
     case ( uid, map ) =>
        val feat_line = new StringBuffer(s"$uid")
        for( wk <- end_week -5 until end_week ){
            if( map.contains( wk ) )
              feat_line.append(s"\t${ map(wk)}")
            else
              feat_line.append(s"\t0")
        }
        feat_line.toString
   }

    feat_result.saveAsTextFile( p.feat_pt)
  }


  val parser = new OptionParser[Params]("Feature"){
    opt[String]("behavior_pt")
      .text("用户行为文件输入路径")
      .action{ (x,c) => c.copy( behavior_pt = x) }
    opt[String]("feat_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( feat_pt = x) }
    opt[Int]("week_id")
      .text("作为样本的周")
      .action{ (x,c) => c.copy( week_id = x) }
    opt[Int]("interval")
      .text("时间窗口（周）")
      .action{ (x,c) => c.copy( interval = x) }
  }

  case class Params(
                    behavior_pt:String = "",
                    feat_pt:String = "",
                    week_id:Int = 9,
                    interval:Int = 9)

  def main(args: Array[String]): Unit = {

    val default_params = Params()
    parser.parse(args, default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
