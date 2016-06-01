package data.extract

import java.io.PrintWriter

import scopt.OptionParser

/**
  * Created by Administrator on 2016/3/9.
  */
object GetRecentUser {

  def run(p:Params): Unit ={
    val end_week = p.week_id
    //获取候选集
    val data = DataLoad.load( p.data_pt ).filter {
      case (uid, week, day, video_site, watch_count) =>
        val new_day = (end_week - week)*7 + 1 - day
        week < end_week && new_day <= p.interval
    }.map{
      x => x._1
    }.distinct

    val writer = new PrintWriter( p.user_pt )
    data.foreach( writer.println )
    writer.close()
  }

  def main(args:Array[String]): Unit ={
    val default_params = Params()
    val parser = new OptionParser[Params](this.getClass.getName) {
      opt[String]("data_pt")
        .text("输入文件")
        .action{ (x,c) => c.copy( data_pt = x )}
      opt[String]("user_pt")
        .text("用户文件")
        .action{ (x,c) => c.copy( user_pt = x )}
      opt[Int]("week_id")
        .text("基准周")
        .action{ (x,c) => c.copy( week_id = x )}
      opt[Int]("interval")
        .text("窗口（天）")
        .action{ (x,c) => c.copy( interval = x )}
    }

    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }
  case class Params(data_pt:String = null,
                    user_pt:String = null,
                    week_id:Int = 0,
                    interval:Int = 0)
}
