package feature

import java.io.PrintWriter

import feature.FeatOHEID.Params
import scopt.OptionParser

import scala.collection.immutable.Iterable

/**
  * Created by Administrator on 2016/2/26.
  */
trait Feature {
  def toMap[T](values: Iterable[(String,Int,T)]):  Map[Int, T] ={
    values.map{
      case (uid,vid, value) =>
        (vid, value )
    }.toMap
  }

  def print[T](path: String,datas:Map[String,Map[Int,T]],
               handle: (Map[Int,T],Int,Int,String) => String,
               handle2: (Int,Int,String) => String): Unit ={
    val feat_out = new PrintWriter(path)
    val keys = datas.keySet.toArray.sorted
    val vid_filter = List( 1, 4, 5, 10)
    Range( 0,7 ).map {
      did =>
        Array(1,10,2,3,4,5,6,7,8,9).map{
          vid =>
            keys.foreach{
              uid =>
                if( datas(uid).contains(vid))
                  feat_out.println( handle(datas(uid), did,vid,uid))
                else if( vid_filter.contains(vid) )
                  feat_out.println( handle2(did,vid,uid) )
            }
        }

    }
    feat_out.close()
  }

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int )

  def run(params: Params): Unit ={
    extract(params.base_pt,
      params.source_name,
      params.feat_name,
      params.week_id,
      params.interval)
  }

  val parser = new OptionParser[Params](FeatGap.getClass.getName){
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
