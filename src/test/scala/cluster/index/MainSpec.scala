package cluster.index

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import commands._
import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = 100//rand.nextInt(2, 10)
    val META_ORDER = 1000//rand.nextInt(2, 10)

    val DATA_MAX = DATA_ORDER*2 - 1
    val DATA_MIN = DATA_MAX/2

    val META_MAX = META_ORDER*2 - 1
    val META_MIN = META_MAX/2

    val meta = new Meta[String, Int, Int](META_MIN, META_MAX)
    val client = new Client[String, Int, Int](DATA_MIN, DATA_MAX, meta)

    def transaction(): Seq[Command[String, Int, Int]] = {

      var data = client.inOrder()
      var commands = Seq[Command[String, Int, Int]]()

      def insert(): Unit = {

        var list = Seq.empty[(Int, Int)]
        val n = rand.nextInt(1, DATA_MIN)

        for(i<-0 until n){
          val k = rand.nextInt(0, 10)

          if(!data.exists(_._1 == k) && !list.exists(_._1 == k)){
            list = list :+ k -> k
          }
        }

        commands = commands :+ Add[String, Int, Int](list)
        data = data ++ list

      }

      def update(): Unit = {
        val len = data.length

        if(len == 0) return

        val values = (if(len < 2) data else scala.util.Random.shuffle(data).slice(0, rand.nextInt(1, len)))
          .map{case (k, _) => k -> rand.nextInt(0, MAX_VALUE)}

        commands = commands :+ Put[String, Int, Int](values)
        data = data.filterNot{case (k, _) => values.exists(_._1 == k)}
        data = data ++ values
      }

      def delete(): Unit = {
        val len = data.length

        if(len == 0) return

        val keys = (if(len < 2) data else scala.util.Random.shuffle(data).slice(0, rand.nextInt(1, len)))
            .map(_._1)

        commands = commands :+ Delete[String, Int, Int](keys)
        data = data.filterNot{case (k, _) => keys.contains(k)}
      }

      val n = rand.nextInt(1, DATA_MIN)

      for(i<-0 until n){
        rand.nextBoolean() match {
          case true => insert()
          case false => rand.nextBoolean() match {
            case true => update()
            case _ => delete()
          }
          //case _ =>
        }
      }

      commands
    }

    val n = 10

    var data = Seq.empty[(Int, Int)]

    for(i<-0 until n){
      val commands = transaction()

      if(client.execute(commands)){

        //println(s"commands: ${commands}\n")

        commands.foreach { cmd =>
          cmd match {
            case Add(list) => data = data ++ list
            case Put(list) =>
              data = data.filterNot{case (k, _) => list.exists(_._1 == k)}
              data = data ++ list

            case Delete(keys) => data = data.filterNot{case (k, _) => keys.contains(k)}
          }
        }
      }
    }

    val dsorted = data.sortBy(_._1)
    val isorted = client.inOrder()

    println(s"dsorted: ${dsorted}\n")
    println(s"isroted: ${isorted}\n")

    println(s"partitions: ${meta.inOrder().map(_._1)}")

    assert(dsorted.equals(isorted))
  }

  "index data " should "be equal to test data" in {

    val n = 1

    for(i<-0 until n){
      test()

    }

  }

}
