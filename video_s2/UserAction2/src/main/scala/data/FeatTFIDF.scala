package data

import feature.FeatUser
import feature.GenFeat.Params
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scopt.OptionParser

import scala.collection.Map

/**
  * 用户对每个网站的tf-idf权重
  */
object FeatTFIDF extends Serializable{

  def createTFIDF(sc:SparkContext, behavior:RDD[(String, Int, Int, Int, Int, String, Int)],
                  visit:RDD[(String, Int, Int, Int, Int, Int, Int)],
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

    val data = join_result.map {
      case (key, ((labels, lcnt), (uid, vid, vcnt))) =>
        ((labels, vid), (1, lcnt, vcnt))
    }

    val tfs: RDD[((String, Int), (Int, Int, Int))]
    = data.reduceByKey {
      case ((v1_1, v1_2, v1_3), (v2_1, v2_2, v2_3)) =>
        (v1_1 + v2_1, v1_2 + v2_2, v1_3 + v2_3)
    }

    val tfs_sum = tfs.map {
      case ((labels, vid), (cnt, lsum, vsum)) =>
        (vid, (cnt, lsum, vsum))
    }.reduceByKey {
      case (x, y) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    }.collectAsMap()

    val tfidf_pairs = tfs.map {
      case ((labels, vid), (cnt, lsum, vsum)) =>
        (labels, List((vid, cnt, lsum, vsum)))
    }.reduceByKey(_ ++ _).map {
      case (labels, list) =>
        val vnum = list.size
        val tfidf = list.map {
          case (vid, cnt, lsum, vsum) =>
            val (cnt_sum, lsum_sum, vsum_sum) = tfs_sum(vid)
            (vid,
              (
                cnt.toDouble / cnt_sum * math.log(10 / vnum + 1),
                lsum.toDouble / lsum_sum * math.log(10 / vnum + 1),
                vsum.toDouble / vsum_sum * math.log(10 / vnum + 1))
              )
        }.toMap
        (labels, tfidf)
    }.saveAsObjectFile(tfidf_pt)
  }
//  //(key,((labels,lcnt),(uid,vid,vcnt)))
//  val feats: Map[String, Predef.Map[Int, (Double, Double, Double)]] =
//    behavior.groupBy(_._1).map{//按照uid group
//    group =>
//      val uid: String = group._1
//      val grp: Iterable[(String, Int, Int, Int, Int, String, Int)] = group._2
//      val feat: Predef.Map[Int, (Double, Double, Double)] =
//         Range(1,11).map{
//          vid =>
//          val sum: (Double, Double, Double) = grp.map{//按照vid group
//            case (uid,week,day,hh,mm,labels,cnt)=>
//               tfidf_pairs(labels)(vid)
//          }.reduceLeft( (x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
//          (vid,sum)
//      }.toMap
//      (uid, feat)
//  }.collectAsMap()
//
//  def getFeat(uid:String, vid:Int): Unit ={
//    if( !feats.contains( uid) || !feats(uid).contains(vid) )  "\t0\t0\t0"
//    else{
//      val (f1,f2,f3) = feats(uid)(vid)
//      s"\t$f1\t$f2\t$f3"
//    }
//  }

  def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("TFIDF")
      .set("spark.hadoop.validateOutputSpces","false")

    if( p.islocal )
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val behavior: RDD[(String, Int, Int, Int, Int, String, Int)]
    = BehaviorExtract.load( sc, p.behavior_pt)

    val visit = VisitExtract.load( sc, p.visit_pt)

    createTFIDF(sc,behavior,visit,p.tfidf_pt)
  }

  val parser = new OptionParser[Params]("TFIDF"){
    opt[String]("visit_pt")
      .text("视频网站访问文件输入路径")
      .action{ (x,c) => c.copy(visit_pt = x) }
    opt[String]("behavior_pt")
      .text("用户行为文件输入路径")
      .action{ (x,c) => c.copy( behavior_pt = x) }
    opt[String]("tfidf_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( tfidf_pt = x) }
    opt[Boolean]("islocal")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( islocal = x) }
  }

  case class Params(visit_pt:String = "",
                    behavior_pt:String = "",
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
