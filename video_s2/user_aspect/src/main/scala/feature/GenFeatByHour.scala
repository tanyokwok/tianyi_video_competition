package feature

import java.io.PrintWriter

import data.extract.DataExtract
import scopt.OptionParser
;

/**
 * 计算用户这两周内的时间特征
 */
object GenFeatByHour extends Feature{

  def extract(base_pt:String, source_name:String,  feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("GenFeats")
    val begin_week = week_id - interval
    val end_week = week_id
    val data: Array[(String, Int, Int, Int, Int, Int, Int)] = DataExtract.extract( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, vedio_site, click_count, hh, mm)=>
        week < week_id && week >= (week_id - interval)
    }

    val candidate = data.map{
      case (uid,week,day,vid, cnt,hh,mm) => uid
    }.toSet.toArray.sorted

    val hour = new FeatByHour(data,week_id,2)

    val path = base_pt + "/" + feat_name
    val feat_out = new PrintWriter(path)
    candidate.foreach{
      uid =>
        val feat_line = new StringBuffer(s"${uid}")
        Range(1,11).foreach {
          vid =>
            feat_line.append(s"${hour.getFeats(uid,vid)}")
        }
        feat_line.append(hour.getUserFeats(uid))
        feat_out.println(feat_line.toString)
    }
    feat_out.close()
  }


  def run(params: Params): Unit ={
    extract(params.base_pt,
      params.source_name,
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
                    feat_name:String = "",
                    week_id:Int = 9,
                    interval:Int = 9)
}
