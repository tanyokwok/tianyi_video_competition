package feature

import java.io.PrintWriter

import data.extract.DataLoad
import scopt.OptionParser

import scala.io.Source

/**
  * Created by Administrator on 2016/3/17.
  */
object TransSpark2Local{

  def loadSparkFeat(spark_feat_pt:String):
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
  def extract(base_pt:String,
              source_name:String,
              spark_feat_pt:String,
              feat_name:String,
              week_id:Int,
              interval:Int ):
  Unit ={
    println("GenFeats")
    val begin_week = week_id - interval
    val end_week = week_id
    val data: Array[(String, Int, Int, Int, Int)] = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }

    val (feat_num, spark_feat) = loadSparkFeat( spark_feat_pt )

    val candidate: Map[String, Set[Int]] = data.map{
      case (uid,week,day,vid, cnt) => (uid, vid)
    }.groupBy( _._1).map{
      group =>
        val uid = group._1
        val vset = group._2.map{
          case (iner_uid, vid) =>
            vid
        }.toSet
        (uid, vset)
    }

    val users = candidate.keys.toArray.sorted
    val path = base_pt + "/" + feat_name
    val feat_out = new PrintWriter(path)
    val vid_filter = List( 1, 4, 5, 10)
    Range( 0,7 ).map {
      did =>
        Array(1,10,2,3,4,5,6,7,8,9).map{
          vid =>
            users.foreach{
              uid =>
                if( candidate(uid).contains(vid) || vid_filter.contains( vid )) {
                  if (spark_feat.contains(uid))
                    feat_out.println( s"$did\t$vid\t$uid\t${spark_feat(uid)}")
                  else{
                    println( s"$uid not hit")
                    val feat_str = new StringBuffer(s"$did\t$vid\t$uid")
                    for( i <-0 until feat_num) feat_str.append(s"\t0")
                    feat_out.println( feat_str.toString )
                  }
                }
            }
        }

    }
    feat_out.close()
  }


  def run(params: Params): Unit ={
    extract(params.base_pt,
      params.source_name,
      params.spark_feat_pt,
      params.feat_name,
      params.week_id,
      params.interval)
  }

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }


  val parser = new OptionParser[Params]("Feature"){
    opt[String]("base_pt")
      .text("输入输出文件工作目录")
      .action{ (x,c) => c.copy(base_pt = x) }
    opt[String]("source_name")
      .text("输入文件名")
      .action{ (x,c) => c.copy( source_name = x) }
    opt[String]("spark_feat_pt")
      .text("spark特征输出")
      .action{ (x,c) => c.copy( spark_feat_pt = x) }
    opt[String]("feat_name")
      .text("特征文件名")
      .action{ (x,c) => c.copy( feat_name = x) }
    opt[Int]("week_id")
      .text("作为样本的周")
      .action{ (x,c) => c.copy( week_id = x) }
    opt[Int]("interval")
      .text("时间窗口（周）")
      .action{ (x,c) => c.copy( interval = x) }
  }

  case class Params(base_pt:String = "",
                    source_name:String = "",
                    spark_feat_pt:String = "",
                    feat_name:String = "",
                    week_id:Int = 9,
                    interval:Int = 9)
}
