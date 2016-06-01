package data.extract

import scopt.OptionParser

import scala.io.Source

/**
 * Created by Administrator on 2016/1/13.
 */
object DataLoad {

  def load( source_pt : String ): Array[(String,Int,Int,Int,Int)] ={
    Source.fromFile( source_pt ).getLines().map{ line =>
      val arr = line.split("\\s+")
      val uid = arr(0)
      val week = arr( 1 ).toInt
      val day = arr(2).toInt
      val vedio_site = arr(3).toInt
      val click_count = arr(4).toInt

      ( uid, week, day, vedio_site, click_count)
    }.toArray
  }

  def run(params: Params): Unit ={
    load(params.input_fp).foreach( x => println( x  ))
  }
  def main(args: Array[String]){
    val default_params = Params()
    val parser = new OptionParser[Params](this.getClass.getName) {
      opt[String]("input_fp")
        .text("输入文件")
        .action{ (x,c) => c.copy( input_fp = x )}
    }

    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }

  case class Params(input_fp: String = "")
}
