package sample

import java.io.PrintWriter

import data.transform.DataTransform.Params
import feature.FeatGap
import scopt.OptionParser

import scala.io.Source

/**
 * Created by Administrator on 2016/1/15.
 */
object ResultConstruct {

  def transform(source_name: String, target_name: String): Unit = {
    val source = Source.fromFile(source_name).getLines.map {
      line =>
        val arr = line.split("\\s+");
        val day = arr(0).toInt
        val vid = arr(1).toInt
        val uid = arr(2)
        val count = arr(3).toInt
        (uid, vid, day, count)
    }.filter {
      case (uid, vid, day, count) =>
        count > 0
    }


    val target = scala.collection.mutable.HashMap[String, Array[Int]]()

    source.foreach {
      case (uid, vid, day, count) =>
        if (!target.contains(uid)) {
          target += Tuple2(uid, Array.fill[Int](70)(0))
        }

        target(uid)(day * 10 + vid - 1) = count
    }

    val target_out = new PrintWriter(target_name)
    target.foreach {
      case (uid, arr) =>
        if( arr.sum != 0 ) {
          target_out.print(uid + "\t")
          val sb = new StringBuffer()
          arr.foreach(x => sb.append(x + ","))
          val str = sb.substring(0, sb.length() - 1)
          if (arr.length != 70)
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
