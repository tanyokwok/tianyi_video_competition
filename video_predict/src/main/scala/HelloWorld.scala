import org.saddle.Vec

/**
 * Created by Administrator on 2016/1/13.
 */
object  HelloWorld {

  def main(args:Array[String]): Unit ={
    val arr = Array(1,2,3,4,5)

    val sb = new StringBuffer()
    arr.foreach( x => sb.append( x + ","))
    println( sb.substring(0, sb.length() - 1 ) )
  }

}
