package feature

import data.{FeatTFIDF, VisitExtract, BehaviorExtract}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
  * Created by Administrator on 2016/3/13.
  */
object GenFeat {

  def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("ActionFeature")
      .set("spark.hadoop.validateOutputSpces","false")

    val sc = new SparkContext(conf)
    val behavior: RDD[(String, Int, Int, Int, Int, String, Int)]
    = BehaviorExtract.load( sc, p.behavior_pt)

    val visit = VisitExtract.load( sc, p.visit_pt)

//    val tfidf = new FeatTFIDF(sc,behavior,visit)
    val user = new FeatUser(behavior)

    val begin_week = p.week_id - p.interval
    val end_week = p.week_id
    val users: RDD[String] =  visit.filter{
      case  ( uid, week, day, vid, cnt, hh, mm)=>
        week < end_week && week >= begin_week
    }.map{ _._1 }.distinct

    val feat_result = users.map{
      uid =>
        val feat_line = new StringBuffer(s"${uid}")
        Range(1,11).map{
          vid=>
//            feat_line.append(s"${tfidf.getFeat(uid,vid)}")
            feat_line.append(s"${user.getFeat(uid)}")
        }
        feat_line.toString
    }

    feat_result.saveAsTextFile( p.feat_pt)
  }


  val parser = new OptionParser[Params]("Feature"){
    opt[String]("visit_pt")
      .text("视频网站访问文件输入路径")
      .action{ (x,c) => c.copy(visit_pt = x) }
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

  case class Params(visit_pt:String = "",
                    behavior_pt:String = "",
                    feat_pt:String = "",
                    week_id:Int = 9,
                    interval:Int = 9)

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
