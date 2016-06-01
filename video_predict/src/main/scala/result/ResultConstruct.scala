package result

import java.io.PrintWriter

import scala.collection.parallel.mutable
import scala.io.Source

/**
 * Created by Administrator on 2016/1/15.
 */
object ResultConstruct {

  def transform( source_name :String, target_name:String ): Unit = {
    val source = Source.fromFile(source_name).getLines.map {
      line =>
        val arr = line.split("\\s+");
        val uid = arr(0)
        val vid = arr(1).toInt
        val day = arr(2).toInt
        val count = arr(3).toInt
        (uid, vid, day, count)
    }.filter{
      case (uid,vid,day,count) =>
        count >0
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
        target_out.print( uid + "\t")
        val sb = new StringBuffer()
        arr.foreach( x => sb.append( x + ","))
        val str = sb.substring(0, sb.length() -1  )
        if( arr.length != 70 )
          println( s"ERROR: $str")
        target_out.println( str )
    }

    target_out.close()

  }

  def main( args:Array[String]): Unit ={
    transform( args(0), args(1) )
//    transform( "E:/video_click/data/tianyi_bd_history_new/pred_base.out","E:/video_click/data/tianyi_bd_history_new/res_base.out")
  }
}
