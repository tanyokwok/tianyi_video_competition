package feature

import java.io.PrintWriter

import data.extract.DataLoad
import feature.FeatOnGap._
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/14.
 */
object FeatStat extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), (Int, Int))])) = {
    val ( uid, vid) = group._1
    val watch_counts = group._2.map( y => y._2._2 ).toArray

    val vec = Vec(  watch_counts )
    val mean = vec.mean
    val median: Double = vec.median
    val stdev = vec.stdev
    (uid,vid,(mean,median,stdev))
  }

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("FeatStat")

    val datas = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < week_id && week >= (week_id - interval)
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( ( week - 1) * 7 +  day, watch_count) )
    }.groupBy( _._1 ).map{ group =>
      calculate(group)
    }.groupBy( _._1 ).map {
      group =>
        val uid = group._1
        val values = toMap[(Double, Double,Double)](group._2)
        (uid,values)
    }//      x._2.foreach( print )

    val path = base_pt + "/" + feat_name
    print[(Double,Double,Double)](
      path, datas, (data,did,vid,uid) =>{
        if( !data.contains( vid )) s"$did\t$vid\t$uid\t0\t0\t0"
        else {
          val (mean, median, stdev) = data(vid)
          s"$did\t$vid\t$uid\t$mean\t$median\t$stdev"
        }
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
