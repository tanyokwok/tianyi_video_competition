package data

import feature.FeatUser
import feature.GenFeat.Params
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scopt.OptionParser

import scala.Predef
import scala.collection.Map

/**
  * 用户对每个网站的tf-idf权重
  */
object TFIDF extends Serializable{

  def createTFIDF(sc:SparkContext, behavior:RDD[(String, Int, Int, Int, Int, String, Int)],
                  visit:RDD[(String, Int, Int, Int, Int, Int, Int)],
                  join_pt:String,
                  tfidf_pt:String) {
    val pairs: Map[String, (String, Int, Int)] = visit.map {
      case (uid, week, day, vid, cnt, hh, mm) =>
        (s"$uid-$week-$day-$hh-$mm", (uid, vid, cnt))
    }.collectAsMap()

    val broadcaseMap = sc.broadcast(pairs)

    val join_result: RDD[(String, ((String, Int), (String, Int, Int)))]
    = behavior.map {
      case (uid, week, day, hh, mm, labels, cnt) =>
        (s"$uid-$week-$day-$hh-$mm", (labels, cnt))
    }.mapPartitions {
      iter =>
        val visit_data: Map[String, (String, Int, Int)] = broadcaseMap.value
        for {
          (key, value) <- iter
          if (visit_data.contains(key))
        } yield (key, (value, visit_data(key)))
    }

    join_result.saveAsObjectFile(join_pt)

    val data = join_result.map {
      case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
        ((labels, vid), (1, lcnt, vcnt))
    }

    //tfs中的key都是唯一的了
    val tfs: RDD[((String, Int), (Int, Int, Int))]
    = data.reduceByKey {
      case ((v1_1, v1_2, v1_3), (v2_1, v2_2, v2_3)) =>
        (v1_1 + v2_1, v1_2 + v2_2, v1_3 + v2_3)
    }

    //找出每个vid中值最大的
    val tfs_max = tfs.map {
      case ((labels, vid), (cnt, lsum, vsum)) =>
        (vid, (cnt, lsum, vsum))
    }.reduceByKey {
      case (x, y) =>
        (
          if(x._1 > y._1) x._1 else y._1,
          if(x._2 > y._2) x._2 else y._2,
          if(x._3 > y._3) x._3 else y._3
          )
    }.collectAsMap()

    val tfidf_pairs: RDD[(String, Predef.Map[Int, (Double, Double, Double)])] = tfs.map {
      case ((labels, vid), (cnt, lsum, vsum)) =>
        (labels, List((vid, cnt, lsum, vsum)))
    }.reduceByKey(_ ++ _).map {
      case (labels, list) =>
        val vnum = list.size
        val tfidf = list.map {
          case (vid, cnt, lsum, vsum) =>
            val (cmax, lmax, vmax) = tfs_max(vid)
            (vid,
              (
                cnt.toDouble / cmax * math.log(12.0 / (vnum + 1)),
                lsum.toDouble / lmax * math.log(12.0 / (vnum + 1)),
                vsum.toDouble / vmax * math.log(12.0 / (vnum + 1))
                )
              )
        }.toMap

        (labels, tfidf)
    }

    tfidf_pairs.saveAsObjectFile(tfidf_pt)
    tfidf_pairs.map{
      case (labels, tfidf) =>
        val line = new StringBuffer(s"$labels")
        tfidf.foreach{
          case (vid, (f1, f2, f3)) =>
            line.append( s"\n$vid\t$f1\t$f2\t$f3")
        }
        line.toString
    }.saveAsTextFile(s"${tfidf_pt}_text")
  }
//  //(key,((labels,lcnt),(uid,vid,vcnt)))


  def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("TFIDF")
      .set("spark.hadoop.validateOutputSpces","false")

    if( p.islocal )
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val behavior: RDD[(String, Int, Int, Int, Int, String, Int)]
    = BehaviorExtract.load( sc, p.behavior_pt)

    val visit = VisitExtract.load( sc, p.visit_pt)

    createTFIDF(sc,behavior,visit,p.join_pt,p.tfidf_pt)
  }

  val parser = new OptionParser[Params]("TFIDF"){
    opt[String]("visit_pt")
      .text("视频网站访问文件输入路径")
      .action{ (x,c) => c.copy(visit_pt = x) }
    opt[String]("behavior_pt")
      .text("用户行为文件输入路径")
      .action{ (x,c) => c.copy( behavior_pt = x) }
    opt[String]("join_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( join_pt = x) }
    opt[String]("tfidf_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( tfidf_pt = x) }
    opt[Boolean]("islocal")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( islocal = x) }
  }

  def loadTFIDF(sc:SparkContext,tfidf_pt:String ):
  RDD[(String, Predef.Map[Int, (Double, Double, Double)])] ={
    val tfidf_pairs:RDD[(String, Predef.Map[Int, (Double, Double, Double)])]
    = sc.objectFile(tfidf_pt)
    tfidf_pairs
  }

  def loadJoin(sc:SparkContext, join_pt:String):
  RDD[(String, ((String, Int), (String, Int, Int)))] ={
    val join_data:RDD[(String, ((String, Int), (String, Int, Int)))]
    = sc.objectFile(join_pt)
    join_data
  }
  case class Params(visit_pt:String = "",
                    behavior_pt:String = "",
                    join_pt:String = "",
                    tfidf_pt:String = "",
                    islocal:Boolean=false)

  def main(args: Array[String]): Unit ={

    val default_params = Params()
    parser.parse(args,default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
