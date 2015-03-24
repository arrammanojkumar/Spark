package learning

/**
 * @author marram
 */
object Factorial {
  def main(args: Array[String]): Unit = {
    println("Enter a Number ( Below 12 only As we are taking in Integer only): ")
    var number = Console.readInt()
    println("Factorial of " + number + " is : " + factorial(number))
  }

  /**
   * It is normal recursion Factorial
   */
  def factorial(n: Int): Int = {
    if (n <= 1)
      1
    else
      n * factorial(n - 1)
  }
}