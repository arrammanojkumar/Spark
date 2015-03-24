package learning

/**
 * @author marram
 */
object TailSumOfNNumbers {
  def main(args: Array[String]): Unit = {
    println("Enter N : ")
    var n = Console.readInt()
    println("Sum of " + n + " is : " + SumOfNNumbers(n, 0))
  }

  @annotation.tailrec
  def SumOfNNumbers(n: Int, acc: Int): Int = {
    if (n >= 0)
      acc
    else {
      println(" n is " + n+" Acc is "+acc)
      SumOfNNumbers(n - 1, n + acc)
    }
  }
}