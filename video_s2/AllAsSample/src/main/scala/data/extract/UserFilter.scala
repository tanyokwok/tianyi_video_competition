package data.extract

import java.io.PrintWriter

import data.extract.DataLoad.Params
import scopt.OptionParser

import scala.io.Source

/**
  * Created by Administrator on 2016/3/8.
  */
object UserFilter {

  def loadUser(p:Params): Set[String] ={
    Source.fromFile(p.user_pt).getLines().map{
      line =>
        val arr = line.split("\\s+")
        arr(0)
    }.toSet
  }
  def filter(p:Params, userSet:Set[String]): Unit ={
    val writer = new PrintWriter(p.output_pt)
    Source.fromFile(p.feat_pt).getLines().filter{
      line =>
        val arr = line.split("\\s+")
        userSet.contains( arr(2) )
    }.foreach{
      writer.println
    }
    writer.close()
  }

  def run(p:Params): Unit ={
    filter(p, loadUser(p) )
  }
  def main(args:Array[String]): Unit ={
    val default_params = Params()
    val parser = new OptionParser[Params](this.getClass.getName) {
      opt[String]("feat_pt")
        .text("特征文件")
        .action{ (x,c) => c.copy( feat_pt = x )}
      opt[String]("user_pt")
        .text("用户文件")
        .action{ (x,c) => c.copy( user_pt = x )}
      opt[String]("output_pt")
        .text("输出文件")
        .action{ (x,c) => c.copy( output_pt = x )}
    }

    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }
  case class Params(feat_pt:String = null,
                    user_pt:String = null,
                    output_pt:String = null)
}
