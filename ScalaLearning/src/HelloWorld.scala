/**
 * @author marram
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    var obj = new hello();
    var obj2 = new hello();

    println("Calling with obj " + obj.hellos);
    println("Calling with obj2 " + obj2.hellos);
  }
}

class hello {
  def hellos {
    println("You have created an object")
  }
}