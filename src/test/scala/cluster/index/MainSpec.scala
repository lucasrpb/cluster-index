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

  val MAX_VALUE = 1000//Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = 50//rand.nextInt(2, 10)
    val META_ORDER = 1000//rand.nextInt(2, 10)

    val DATA_MAX = DATA_ORDER*2 - 1
    val DATA_MIN = DATA_MAX/2

    val META_MAX = META_ORDER*2 - 1
    val META_MIN = META_MAX/2

    val meta = new Meta[String, Int, Int](META_MIN, META_MAX)
    val client = new Client[String, Int, Int](DATA_MIN, DATA_MAX, meta)

    var DATA = Seq.empty[(Int, Int)]

    def insert(): Unit = {

      val data = client.inOrder()

      val n = rand.nextInt(1, DATA_MIN)
      var list = Seq.empty[(Int, Int)]

      for(i<-0 until n){
        val k = rand.nextInt(0, MAX_VALUE)

        if(!data.exists(_._1 == k) && !list.exists(_._1 == k)){
          list = list :+ k -> k
        }
      }

      if(client.insert(list)){
        DATA = DATA ++ list
      }
    }

    def remove(): Unit = {
      val data = client.inOrder()

      val len = data.length
      val keys = scala.util.Random.shuffle(if(len <= 2) data else data.slice(0, rand.nextInt(2, data.length)))
        .map(_._1)

      if(client.remove(keys)){
        DATA = DATA.filterNot {case (k, _) => keys.contains(k)}
      }
    }

    def update(): Unit = {
      val data = client.inOrder()

      val len = data.length
      val values = scala.util.Random.shuffle(if(len <= 2) data else data.slice(0, rand.nextInt(2, data.length)))
        .map{case (k, _) => k -> rand.nextInt(0, MAX_VALUE)}

      if(client.update(values)){
        DATA = DATA.filterNot {case (k, _) => values.exists(_._1 == k)}
        DATA = DATA ++ values
      }
    }

    val n = 100

    for(i<-0 until n){
      rand.nextBoolean() match {
        case true => insert()
        case false => rand.nextBoolean() match {
          case true => update()
          case false => remove()
        }
      }
    }

    val dsorted = DATA.sortBy(_._1)
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
