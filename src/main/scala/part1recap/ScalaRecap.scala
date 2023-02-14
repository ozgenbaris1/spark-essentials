package part1recap

object ScalaRecap extends App {
  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions
  val theUnit = println("Hello, Scala") // Unit -> no meaning value (void)

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")

  }

  // singleton pattern
  object MySingleton

  /*
    companions:
    a class/trait and a singleton object with same name -> companion
   */
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // functional programming
  val incrementer: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }

  val incrementerLambda: Int => Int = x => x + 1 // equivalent

  val incremented = incrementer(42)

  // map, flatMap, filter
  val processedList = List(1, 2, 3).map(incrementer) // List(2, 3, 4)

  // pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "some returned value"
    case _                       => "something else"
  }

  // future
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.util.Success
  import scala.util.Failure

  val aFuture = Future {
    // some expensive computation which runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) =>
      println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I've failed: $ex")
  }

  // partial funtions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // implicits
  // auto intected by the compiler

  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implicitInt = 67

  val implicitCall = methodWithImplicitArgument
  // equivalent to methodWithImplicitArgument(67)

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet // automatically converts to fromStringToPerson("Bob").greet

  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Lassie".bark

  /*
    - local scope
    - imported scope
    - companion objects of the types involved in the method call
   */

}
