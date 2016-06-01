package data.transform

import java.io.PrintWriter

import data.extract.DataExtract
import scopt.OptionParser

/**
  * Created by Administrator on 2016/2/25.
  */
object DataTransform {

  def run(params:Params) {
    val datas = DataExtract.extract(params.input_fp)
    val output = new PrintWriter( params.output_fp)
    datas.foreach{
      case ( uid, week, day, video_site, click_count, hh, mm) =>
        output.println(s"$uid\t$week\t$day\t$video_site\t$click_count\t$hh\t$mm")
    }
    output.close()

    //按照天合并每个用户点击视频的数量，得到用户每天点击某个视频网站的次数
    val data_trans = datas.map{
      case ( uid, week, day, video_site, click_count, hh, mm) =>
        ((uid,week,day,video_site), click_count, hh, mm)
    }.groupBy( _._1 )//按照(uid,week,day,video_site)分组
      .map {
      group =>
        val ( uid, week, day, vid ) = group._1
        //group._2表示Array[(uid,week,day,vid),click_count,hh,mm]
        //x._2 = click_count
        val click_count = group._2.map( x => x._2 ).sum

        ( uid, week, day, vid, click_count )
    }.toArray.sortBy( x => x._1 + " " + x._2 + " " + x._3)

    val out_trans = new PrintWriter( params.group_fp)

    data_trans.foreach{
      case ( uid, week, day, video_site, click_count) =>
        out_trans.println(s"$uid\t$week\t$day\t$video_site\t$click_count")
    }

    out_trans.close()

  }

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    val parser = new OptionParser[Params](DataTransform.getClass.getName){
      opt[String]("input_fp")
        .text("Input path of data file")
        .action{ (x,c) => c.copy(input_fp = x) }
      opt[String]("output_fp")
        .text("Output path of data file")
        .action{ (x,c) => c.copy(output_fp = x) }
      opt[String]("group_fp")
        .text("用户每天点击某个视频网站的次数的输出文件")
        .action{ (x,c) => c.copy(group_fp = x) }
    }

    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  case class Params(input_fp: String = "",
                    output_fp: String = "",
                    group_fp: String = "")
}
