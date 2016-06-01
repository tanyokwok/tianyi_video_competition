package feature

import data.extract.DataLoad
import feature.FeatOnGap._
import scopt.OptionParser

/**
  * Created by Administrator on 2016/1/16.
  */
object FeatOHEID extends Feature{

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println( "FeatOHTID")
    //获取候选集
    val candidate = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count )
      => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }

    //计算特征并按照vid映射
    val datas = candidate.groupBy( _._1 ).map { x =>
      val (uid, vid) = x._1
      (uid, vid, ())
    }.groupBy( _._1 ).map{
      group =>
        val uid = group._1
        val values = toMap[Unit](group._2)
        (uid, values)
    }

    val path = base_pt + "/" + feat_name

    print[Unit](
      path, datas, (data,did,vid,uid) =>{
        val feat_line = new StringBuilder(s"$did\t$vid\t$uid")

        for( i <- 0 until 7 ){
          if( did == i ) feat_line.append("\t1")
          else feat_line.append("\t0")
        }

        for( i <- 1 to 10 ){
          if( vid == i ) feat_line.append("\t1")
          else feat_line.append("\t0")
        }

        feat_line.toString()
      }
    )
  }

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }

}
