package sample
import scala.util.matching.Regex

object Test {
  def main(args: Array[String]): Unit = {
    val string = "Thieu Hai. 1996 Hoan"
    val re = " ([A-Za-z]+)\\.".r
    val a = re.findAllIn(string)
    println(a)
  }
}
