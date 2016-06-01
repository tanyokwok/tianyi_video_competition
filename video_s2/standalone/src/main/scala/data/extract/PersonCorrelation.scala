package data.extract

import org.saddle.Vec
import scopt.OptionParser

/**
  * Created by Administrator on 2016/5/7.
  */
object PersonCorrelation {

  def corr(a: Vec[Int], b: Vec[Int]): Double = {
    if (a.length != b.length) sys.error("vector length is not equal.")

    val len = a.length
    val amean = a.mean
    val astdev = a.stdev
    val bmean = b.mean
    val bstdev = b.stdev

    // * 每个相对应位置相乘仍然是一个向量， dot 点积最后是一个值
    1.0 / (len - 1.0) * (((a - amean) / astdev) dot ((b - bmean) / bstdev))

  }

  def run(params: Params): Unit = {
    // < (uid,vid), cnt > 用户视频网站 共现次数
    val data = DataLoad.load(params.input_fp).filter{
      case (uid, week, day, vedio_site, click_count) =>
        week >= params.week_begin && week <= params.week_end
    }.map {
      case (uid, week, day, vedio_site, click_count) =>
        ((uid, vedio_site), click_count)
    }.groupBy(_._1).map {
      group =>
        val (uid, vid) = group._1
        val sum = group._2.map {
          case (key, cnt) =>
            cnt
        }.sum
        ((uid, vid), sum)
    }
    //用户列表
    val users = data.map {
      case ((uid, vid), sum) => uid
    }.toSet.toArray

    val vecs: Array[Vec[Int]] = Range(1, 11).map {
      vid =>
        val vec = users.map {
          uid =>
            if (!data.contains((uid, vid))) 0
            else data((uid, vid))
        }
        Vec(vec)
    }.toArray

    for( i <- 0 until 10 ){
      for( j <- 0 until 10 ){
        var a = vecs(i)
        var b = vecs(j)
    //    if( i == 2 ) a = -a
    //    if( j == 2 ) b = -b
        print( s"${corr( a, b)} ")
      }
      println()
    }
  }

  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params](this.getClass.getName) {
      opt[String]("input_fp")
        .text("输入文件")
        .action { (x, c) => c.copy(input_fp = x) }
      opt[Int]("week_begin")
        .text("起始周")
        .action { (x, c) => c.copy( week_begin= x) }
      opt[Int]("week_end")
        .text("结束周")
        .action { (x, c) => c.copy( week_end= x) }
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }

  case class Params(input_fp: String = "",
                    week_begin: Int = 0,
                    week_end: Int = 0)

}
