package feature

import data.extract.DataLoad
import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/14.
 */
object FeatStatDecay extends Feature{

  def calculate(group: ((String, Int), Array[((String, Int), (Int, Int))])) = {
    val ( uid, vid) = group._1
    val watch_counts = scala.collection.mutable.MutableList[Int]()
    group._2.foreach{
      case ((uid, vid), (week, count)) =>
        for( i <- 0 until math.pow(2,week).toInt ){
          watch_counts+= count
        }
    }
    //      watch_counts.foreach(println)
    val vec = Vec(  watch_counts.toArray )
    val mean = vec.mean
    val median = vec.median
    val stdev = vec.stdev
    (uid,vid,(mean,median,stdev))
  }

  def extract(base_pt:String, source_name:String, feat_name:String, week_id:Int, interval:Int ): Unit ={
    println("FeatStatDecay")
    val begin_week = week_id - interval
    val end_week = week_id
    val datas = DataLoad.load( base_pt + "/" + source_name ).filter{
      case ( uid, week, day, video_site, watch_count ) =>
        week < end_week && week >= begin_week
    }.map{
      case ( uid, week, day, video_site, watch_count ) => ( (uid , video_site) ,( week - begin_week, watch_count) )
    }.groupBy( _._1 ).map { group =>
      calculate(group)
    }.groupBy( _._1 ).map{
      group =>
        val uid = group._1
        val values = toMap[(Double,Double,Double)](group._2)
        (uid, values)
    }


    val path = base_pt + "/" + feat_name
    print[(Double,Double,Double)](
      path, datas, (data,did,vid,uid) =>{
        val (mean,median,stdev) = data(vid)
        s"$did\t$vid\t$uid\t$mean\t$median\t$stdev"
      },
      (did,vid,uid)=>{
        s"$did\t$vid\t$uid\t0\t0\t0"
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
