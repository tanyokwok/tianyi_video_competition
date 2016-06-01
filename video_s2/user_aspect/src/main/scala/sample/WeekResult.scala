package sample

import java.io.PrintWriter

import scopt.OptionParser

import scala.io.Source

/**
 * Created by Administrator on 2016/1/15.
 */
object WeekResult {

  def transform(source_name: String, target_name: String): Unit = {
    val source = Source.fromFile(source_name).getLines.map {
      line =>
        val arr = line.split("\\s+");
        val uid = arr(0)
        val vid = arr(1).toInt
        val count = arr(2).toInt
        (uid, vid, count)
    }.filter {
      case (uid, vid, count) =>
        count > 0
    }


    val target = scala.collection.mutable.HashMap[String, Array[Int]]()

    source.foreach {
      case (uid, vid, count) =>
        if (!target.contains(uid)) {
          target += Tuple2(uid, Array.fill[Int](10)(0))
        }

        target(uid)( vid - 1) = count
    }

    val target_out = new PrintWriter(target_name)
    target.foreach {
      case (uid, arr) =>
        if( arr.sum != 0 ) {
          target_out.print(uid + "\t")
          val sb = new StringBuffer()
          arr.foreach(x => sb.append(x + ","))
          val str = sb.substring(0, sb.length() - 1)
          if (arr.length != 10)
            println(s"ERROR: $str")
          target_out.println(str)
        }
    }

    target_out.close()

  }

  def main(args: Array[String]): Unit = {
    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => transform(params.source_pt, params.target_pt)
      case None => System.exit(1)
    }
  }

  val parser = new OptionParser[Params]("ResultConstruct") {
    opt[String]("source_pt")
      .text("输入文件路径")
      .action { (x, c) => c.copy(source_pt = x) }
    opt[String]("target_pt")
      .text("输出文件路径")
      .action { (x, c) => c.copy(target_pt = x) }
  }

  case class Params(source_pt: String = "", target_pt: String = "")

}
