package utils

/**
 * Created by Administrator on 2016/1/12.
 */
object Utils {
  def reduceByKey[K,V](collection: Traversable[Tuple2[K, V]])(implicit num: Numeric[V]) = {
    import num._
    collection
      .groupBy(_._1)
      .map { case (group: K, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)} }
  }
  import scala.annotation.tailrec

  @tailrec def findKMedian(arr: Array[Double], k: Int)(implicit choosePivot: Array[Double] => Double): Double = {
    val a = choosePivot(arr)
    val (s, b) = arr partition (a >)
    if (s.size == k) a
    // The following test is used to avoid infinite repetition
    else if (s.isEmpty) {
      val (s, b) = arr partition (a ==)
      if (s.size > k) a
      else findKMedian(b, k - s.size)
    } else if (s.size < k) findKMedian(b, k - s.size)
    else findKMedian(s, k)
  }

  def findMedian(arr: Array[Double])(implicit choosePivot: Array[Double] => Double) = findKMedian(arr, (arr.size - 1) / 2)

  def main(args:Array[String]): Unit ={
    val  col = List((("some","key"),100), (("some","key"),100), (("some","other","key"),50))
    reduceByKey(col).foreach{
      x => println(x )
    }
  }

  def testGroupBy( collection: Traversable[ Tuple2[String,Int] ] ): Unit ={
    collection.groupBy( _._1)
  }
}
