package nmf
import data.BehaviorExtract
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.Map
import scala.collection.immutable.Iterable

/**
  * Created by Administrator on 2016/3/17.
  */
object GenUserLabelPCA extends serializable{
 def extract(p:Params): Unit ={
    val conf = new SparkConf().setAppName("UserLabelMatrix")
      .set("spark.hadoop.validateOutputSpces","false")

    if( p.islocal )
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val begin_week = p.week_id - p.interval
    val end_week = p.week_id

    val data = BehaviorExtract.load( sc, p.behavior_pt)
    val users: Array[(String, Long)] = data.map{
      case (uid, week, day, hh, mm, labels, cnt) =>
         uid
    }.distinct.zipWithIndex().collect()

   val users_map = users.map{
     case (uid,id)=>
       (id,uid)
   }.toMap
   println( s"n(user)=${users.size}" )
    val behavior
    = data.filter{
      case (uid, week, day, hh, mm, labels, cnt) =>
        week < end_week && week >= begin_week
    }.map{
      case (uid, week, day, hh, mm, labels, cnt) =>
        ((uid,labels), cnt)
    }.reduceByKey(_ + _)

   val user_action: RDD[((String, Map[String, Int]), Long)] = behavior.map{
     case ((uid,labels), cnt) =>
       (labels,Map( (uid,cnt) ))
   }.reduceByKey( _ ++ _ ).zipWithIndex()

   val label_map: Map[Long,String] = user_action.map{
     case ((label,map),id)=>
       (id,label)
   }.collectAsMap()

  println(s"n(label)=${label_map.size}")
  val train_points: RDD[Vector] =user_action.map{
     case ( (label, map),id ) =>
       val arr = for{
         (uid_str,uid) <- users
         if( map.contains( uid_str ))
       }yield (uid.toInt, map(uid_str).toDouble )

       val seq = arr.toSeq
      Vectors.sparse(users.size, seq)
   }

   val rowMat = new RowMatrix( train_points )

   val pc: Matrix = rowMat.computePrincipalComponents( p.component )
   val rows = pc.numRows
   val cols = pc.numCols

   println( s"n(row)=${rows}, n(col)=${cols}" )
   val feat_seq = Range(0,rows).map{
     i=>
     val uid = users_map( i )
     val feat = new StringBuffer(uid)
     for( j <- 0 until cols ){
       feat.append(s"\t${pc(i,j)}")
     }

     feat.toString
   }

   val feat_rdd = sc.parallelize(feat_seq)

   feat_rdd.saveAsTextFile(p.feat_pt)
  }


  val parser = new OptionParser[Params]("Feature"){
    opt[String]("behavior_pt")
      .text("用户行为文件输入路径")
      .action{ (x,c) => c.copy( behavior_pt = x) }
    opt[String]("feat_pt")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( feat_pt = x) }
    opt[Int]("component")
      .text("成分的个数")
      .action{ (x,c) => c.copy( component = x) }
    opt[Int]("week_id")
      .text("作为样本的周")
      .action{ (x,c) => c.copy( week_id = x) }
    opt[Int]("interval")
      .text("时间窗口（周）")
      .action{ (x,c) => c.copy( interval = x) }
    opt[Boolean]("islocal")
      .text("特征文件输出路径")
      .action{ (x,c) => c.copy( islocal = x) }
  }

  case class Params(
                    behavior_pt:String = "",
                    feat_pt:String = "",
                    component:Int = 10,
                    week_id:Int = 9,
                    interval:Int = 9,
                    islocal:Boolean=false)

  def main(args: Array[String]): Unit = {

    val default_params = Params()
    parser.parse(args, default_params) match {
      case Some(params) => extract(params)
      case None => System.exit(1)
    }

  }
}
