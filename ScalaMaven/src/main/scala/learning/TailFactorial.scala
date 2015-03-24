package learning

/**
 * @author marram
 * 
 * Factorial Program using Tail Recursion
 */

object TailFactorial {
  def main(args: Array[String]): Unit = {
    println(factorial(5, 1))
  }
  
 @annotation.tailrec
 def factorial(number : Int, acc : Int) : Int = {
    if(number == 0) 
      acc
    else
      factorial(number-1, number * acc)
  }
}