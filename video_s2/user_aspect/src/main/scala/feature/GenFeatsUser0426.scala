package feature

import java.io.PrintWriter

import data.extract.DataLoad
import scopt.OptionParser
;

/**
 * Created by Administrator on 2016/3/12.
 */
object GenFeatsUser0426 extends Feature{

  def extract(base_pt:String,
              group_data:String,
              feat_name:String,
              week_id:Int, interval:Int ): Unit ={
    val begin_week = week_id - interval
    val end_week = week_id
    val data: Array[(String, Int, Int, Int, Int)] = DataLoad.load( base_pt + "/" + group_data).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }

    val avgGap = new FeatAvgGap()
    val gap = new FeatGap()
//    val ongap = new FeatOnGapCount()
    val stat = new FeatStat()
    val week = new FeatWeek()
//    val user = new FeatUser()
//    val newUser = new FeatNewUser()
    avgGap.featGen(data,week_id)
    gap.featGen(data,week_id)
//    ongap.featGen(data,week_id)
    stat.featGen(data)
    week.featGen(data,begin_week,end_week)
//    user.featGen(data)
//    newUser.featGen(data)

    val path = base_pt + "/" + feat_name
    val user_keys = data.groupBy(_._1).keySet.toArray.sorted
    val feat_out = new PrintWriter(path)
    user_keys.foreach{
      uid =>
        val feat_line = new StringBuffer(s"${uid}")
        Range(1,11).foreach{
          vid =>
            feat_line.append(s"${avgGap.getFeat(uid,vid)}")
            feat_line.append(s"${gap.getFeat(uid,vid)}")
//            feat_line.append(s"${ongap.getFeat(uid,vid,week_id)}")
            feat_line.append(s"${stat.getFeat(uid,vid)}")
            feat_line.append(s"${week.getFeat(uid,vid,begin_week,end_week)}")
//            feat_line.append(s"${hour.getFeats(uid,vid)}")
        }
//        feat_line.append( s"${user.getFeat(uid,begin_week,end_week)}")
//        feat_line.append(s"${newUser.getFeat(uid)}")
        feat_out.println( feat_line.toString)
    }
    feat_out.close()

  }


  val parser = new OptionParser[Params]("Feature"){
    opt[String]("base_pt")
      .text("输入输出文件工作目录")
      .action{ (x,c) => c.copy(base_pt = x) }
    opt[String]("group_data")
      .text("group_data输入文件名")
      .action{ (x,c) => c.copy( group_data = x) }
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
                    group_data:String = "",
                    feat_name:String = "",
                    week_id:Int = 9,
                    interval:Int = 9)
  def run(params: Params): Unit ={
    extract(params.base_pt,
      params.group_data,
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
}
