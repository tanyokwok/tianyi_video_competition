package split_train_test

import java.io.PrintWriter

import data.extract.{DataLoad, DataExtract}

/**
 * Created by Administrator on 2016/1/18.
 */
object CutOddVal {

    def split(base_pt: String, source_name: String, output_name:String ,threshold:Int): Unit = {
      println( this.getClass.getCanonicalName )
      val out = new PrintWriter(base_pt + "/" + output_name )

      DataLoad.load(base_pt + "/" + source_name).foreach {
        case (uid, week, day, video_site, watch_count) =>
          if( watch_count > threshold ){
            out.println( s"$uid\t$week\t$day\t$video_site\t$threshold" )
          }else
            out.println( s"$uid\t$week\t$day\t$video_site\t$watch_count" )
      }

      out.close()
    }

    def main(args: Array[String]): Unit = {
      split("E:/video_click/data/tianyi_bd_history_new", "online_train", "online_cut_train", 10)
    }
}
