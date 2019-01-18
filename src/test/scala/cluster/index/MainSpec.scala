package cluster.index

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import index._
import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = 1000//Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = 10//rand.nextInt(2, 10)
    val META_ORDER = 1000//rand.nextInt(2, 10)

    val DATA_MAX = DATA_ORDER*2 - 1
    val DATA_MIN = DATA_MAX/2

    val META_MAX = META_ORDER*2 - 1
    val META_MIN = META_MAX/2

    var data = Seq.empty[(Int, Int)]

    val meta = new Meta[String, Int, Int](META_MIN, META_MAX)
    val client = new Client[String, Int, Int](DATA_MIN, DATA_MAX, meta)

    def insert(): Unit = {

      val n = rand.nextInt(1, DATA_MIN)
      var list = Seq.empty[(Int, Int)]

      for(i<-0 until n){
        val k = rand.nextInt(0, MAX_VALUE)

        if(!data.exists(_._1 == k) && !list.exists(_._1 == k)){
          list = list :+ k -> k
        }
      }

      if(client.insert(list)){
        data = data ++ list
      }
    }

    val n = 10

    for(i<-0 until n){
      rand.nextBoolean() match {
        case _ => insert()
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

    val n = 1000

    for(i<-0 until n){
      test()

    }

  }

}
