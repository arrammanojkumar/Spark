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