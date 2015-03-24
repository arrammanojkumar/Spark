package learning

/**
 * @author marram
 *
 *
 * This class have all the examples related to TailRecursion
 *
 */
object SampleExamples {
  def main(args: Array[String]): Unit = {
    println(perfectSquare(9))
  }

  /**
   * It returns an Integer which is a Sum Of N Numbers
   *
   */
  def sumOfNNumbers(n: Int): Int = {
    @annotation.tailrec
    def SumOfNNumbers(n: Int, acc: Int): Int = {
      if (n <= 0)
        acc
      else {
        SumOfNNumbers(n - 1, n + acc)
      }
    }
    SumOfNNumbers(n, 0)
  }

  /**
   * It returns a Factorial of a given number
   */
  def factorial(n: Int): Int = {
    @annotation.tailrec
    def Factorial(number: Int, acc: Int): Int = {
      if (number == 0)
        acc
      else
        Factorial(number - 1, number * acc)
    }
    Factorial(n, 1)
  }

  /**
   * It returns Sum of factors of a Given Number
   */
  def sumOfFactorsOfN(n: Int): Int = {
    @annotation.tailrec
    def SumOfFactorsOfN(n: Int, curr: Int, acc: Int): Int = {
      if (curr == n)
        acc
      else {
        if (n % curr == 0)
          SumOfFactorsOfN(n, curr + 1, acc + curr)
        else
          SumOfFactorsOfN(n, curr + 1, acc)
      }
    }
    SumOfFactorsOfN(n, 1, 0)
  }

  /**
   * It Returns True if a given number is Perfect Number
   * else it returns false
   */
  def perfectNumber(n: Int): Boolean = {
    @annotation.tailrec
    def SumOfFactorsOfN(n: Int, curr: Int, acc: Int): Int = {
      if (curr == n)
        acc
      else {
        if (n % curr == 0)
          SumOfFactorsOfN(n, curr + 1, acc + curr)
        else
          SumOfFactorsOfN(n, curr + 1, acc)
      }
    }
    (n == SumOfFactorsOfN(n, 1, 0))
  }

  /**
   * It Returns True if a given number is Perfect Square
   * else Returns false
   */
  def perfectSquare(n: Int): Boolean = {
    @annotation.tailrec
    def PerfectSquare(n: Int, acc: Int): Boolean = {
      if ((n / 2 < acc))
        false
      else if ((acc * acc == n))
        true
      else
        PerfectSquare(n, acc + 1)
    }
    PerfectSquare(n, 1)
  }
}