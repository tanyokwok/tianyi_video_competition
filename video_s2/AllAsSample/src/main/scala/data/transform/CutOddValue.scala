package data.transform

import java.io.PrintWriter

import data.extract.DataLoad
import data.extract.DataLoad.Params
import scopt.OptionParser

object CutOddValue {

  def split(base_pt: String, source_name: String, output_name: String, threshold: Int): Unit = {
    println(this.getClass.getCanonicalName)
    val out = new PrintWriter(base_pt + "/" + output_name)

    DataLoad.load(base_pt + "/" + source_name).foreach {
      case (uid, week, day, video_site, watch_count) =>
        if (watch_count > threshold) {
          out.println(s"$uid\t$week\t$day\t$video_site\t$threshold")
        } else
          out.println(s"$uid\t$week\t$day\t$video_site\t$watch_count")
    }

    out.close()
  }

  def run(params: Params): Unit = {
    split(params.base_pt, params.source_name, params.output_name, params.threshold)
  }

  def main(args: Array[String]): Unit = {

    val default_params = Params()
    val parser = new OptionParser[Params](this.getClass.getName) {
      opt[String]("base_pt")
        .text("输入输出文件工作目录")
        .action { (x, c) => c.copy(base_pt = x) }
      opt[String]("source_name")
        .text("输入文件名")
        .action { (x, c) => c.copy(source_name = x) }
      opt[String]("output_name")
        .text("输出文件名")
        .action { (x, c) => c.copy(output_name = x) }
      opt[Int]("threshold")
        .text("截断阈值")
        .action { (x, c) => c.copy(threshold = x) }
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }

  case class Params(base_pt: String = "",
                    source_name: String = "",
                    output_name: String = "",
                    threshold: Int = 10)

}
