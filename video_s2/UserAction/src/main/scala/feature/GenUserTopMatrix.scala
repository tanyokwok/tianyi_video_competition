package feature

import data.BehaviorExtract
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.Map

/**
  * Created by Administrator on 2016/3/17.
  */
object GenUserTopMatrix extends serializable{


  def loadJoin(sc:SparkContext, join_pt:String):
  RDD[(String, ((String, Int), (String, Int, Int)))] ={
    val join_data:RDD[(String, ((String, Int), (String, Int, Int)))]
    = sc.objectFile(join_pt)
    join_data
  }
 def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("UserLabelMatrix")
      .set("spark.hadoop.validateOutputSpces","false")

    val sc = new SparkContext(conf)

    val begin_week = p.week_id - p.interval
    val end_week = p.week_id
    val join_RDD: RDD[(String, ((String, Int), (String, Int, Int)))]
    = loadJoin(sc, p.join_pt )

    val stat: RDD[(String, Int)] = join_RDD.filter{
      case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
        val arr = key.split("-")
        val week = arr(1).toInt
       week < end_week && !labels.startsWith("视频")
    }.map{
      case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
        (labels,1)
    }.reduceByKey( _ + _ ).sortBy( -_._2)

   stat.map{
     case (label,cnt)=>
       s"$label\t$cnt"
   }.saveAsTextFile( p.label_pt )

    val label_keys: Set[String] = stat.take(p.top).map {
      case (label, cnt) =>
        label
    }.toSet

    val data: RDD[(String, Int, Int, Int, Int, String, Int)] = BehaviorExtract.load( sc, p.behavior_pt).filter{
      case (uid, week, day, hh, mm, labels, cnt) =>
        label_keys.contains( labels )
    }

   genFeat(data,begin_week ,end_week,label_keys, p.feat1_pt)
   genFeat(data,begin_week+1 ,end_week+1,label_keys, p.feat2_pt)
}


  def genFeat(data: RDD[(String, Int, Int, Int, Int, String, Int)],
              begin_week:Int, end_week:Int,
              label_keys: Set[String],
              feat_pt:String): Unit ={

    val behavior
    = data.filter{
      case (uid, week, day, hh, mm, labels, cnt) =>
        ( week < end_week && week >= begin_week)
    }.map{
      case (uid, week, day, hh, mm, labels, cnt) =>
        ((uid,labels), cnt)
    }.reduceByKey(_ + _)

  val feat_result: RDD[String] = behavior.map{
     case ((uid,labels), cnt) =>
       (uid,Map( (labels,cnt) ))
   }.reduceByKey( _ ++ _ ).map{
     case ( uid, map ) =>
        val feat_line = new StringBuffer(s"$uid")
        label_keys.foreach{
          labels =>
            if( map.contains( labels ) )
              feat_line.append(s"\t${ map(labels)}")
            else
              feat_line.append(s"\t0")
        }
        feat_line.toString
   }

    feat_result.saveAsTextFile( feat_pt)

  }
  val parser = new OptionParser[Params]("Feature"){
    opt[String]("join_pt")
      .text("join文件输入路径")
      .action{ (x,c) => c.copy( join_pt = x) }
    opt[String]("behavior_pt")
      .text("用户行为文件输入路径")
      .action{ (x,c) => c.copy( behavior_pt = x) }
    opt[String]("label_pt")
      .text("用户行为文件输入路径")
      .action{ (x,c) => c.copy( label_pt = x) }
    opt[String]("feat1_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( feat1_pt = x) }
    opt[String]("feat2_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( feat2_pt = x) }
    opt[Int]("week_id")
      .text("作为样本的周")
      .action{ (x,c) => c.copy( week_id = x) }
    opt[Int]("interval")
      .text("时间窗口（周）")
      .action{ (x,c) => c.copy( interval = x) }
   opt[Int]("top")
      .text("时间窗口（周）")
      .action{ (x,c) => c.copy( top = x) }
  }

  case class Params(join_pt:String="",
                    behavior_pt:String = "",
                    label_pt:String ="",
                    feat1_pt:String = "",
                    feat2_pt:String = "",
                    week_id:Int = 9,
                    interval:Int = 9,
                    top:Int = 100)

  def main(args: Array[String]): Unit = {

    val default_params = Params()
    parser.parse(args, default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
