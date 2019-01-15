package cluster.index

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = 1000//Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER_PARTITION = 4//rand.nextInt(2, 10)
    val META_ORDER_PARTITION = 4//rand.nextInt(2, 10)

    val DATA_ORDER_META = 10//rand.nextInt(2, 10)
    val META_ORDER_META = 10//rand.nextInt(2, 10)

  }

  "index data " should "be equal to test data" in {

    val n = 1

    for(i<-0 until n){
      test()

    }

  }

}
