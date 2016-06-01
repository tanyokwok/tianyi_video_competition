package feature

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


}
