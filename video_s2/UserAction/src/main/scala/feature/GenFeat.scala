package feature

import data.{TFIDF, VisitExtract, BehaviorExtract}
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

    val begin_week = p.week_id - p.interval
    val end_week = p.week_id

    val behavior: RDD[(String, Int, Int, Int, Int, String, Int)]
    = BehaviorExtract.load( sc, p.behavior_pt).filter{
      case (uid, week, day, hh, mm, labels, cnt) =>
        week < end_week && week >= begin_week
    }

    val join_rdd:RDD[(String, ((String, Int), (String, Int, Int)))]
    = TFIDF.loadJoin(sc, p.join_pt).filter{
      case (key,value) =>
        val arr = key.split("-")
        val week = arr(1).toInt
        week < end_week && week >= begin_week
    }

    val prob_rdd:RDD[((String, Int), (Double, Double, Double))]
    = sc.objectFile( p.prob_pt )

    val user = new FeatUser(behavior)
    val prob = new FeatProb(join_rdd,prob_rdd)

    val users: RDD[String] =   VisitExtract.load( sc, p.visit_pt).filter{
      case  ( uid, week, day, vid, cnt, hh, mm)=>
        week < end_week && week >= begin_week
    }.map{ _._1 }.distinct

    val feat_result = users.map{
      uid =>
        val feat_line = new StringBuffer("")
        Range(1,11).map{
          vid=>
            feat_line.append(s"${prob.getFeat(uid,vid)}")
        }
        feat_line.append(s"${user.getFeat(uid)}")
        s"$uid\t" + feat_line.toString.substring(1)
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
    opt[String]("join_pt")
      .text("visit和behavior的连接表")
      .action{ (x,c) => c.copy( join_pt = x) }
    opt[String]("prob_pt")
      .text("label的概率表")
      .action{ (x,c) => c.copy( prob_pt = x) }
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
                    join_pt:String = "",
                    prob_pt:String = "",
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
