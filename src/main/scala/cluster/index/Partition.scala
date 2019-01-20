package cluster.index

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import commands._

import scala.collection.concurrent.TrieMap
import scala.collection.script.Remove
import scala.reflect.ClassTag

class Partition[T: ClassTag, K: ClassTag, V: ClassTag](val MIN: Int,
                                                       val MAX: Int,
                                                       val meta: Meta[T, K, V])(implicit val ord: Ordering[K]) {

  val root = new AtomicReference[Block[T, K, V]](new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T],
    MIN, MAX))

  def execute(cmd: Command[T, K, V], index: Block[T, K, V]): Boolean = {
    cmd match {
      case Add(data) =>
        val ok = index.insert(data)._1

        if(!ok)
        println(s"add => ${ok}\n")

        ok
      case Put(data) =>
        val ok = index.update(data)._1

        if(!ok)
        println(s"put => ${ok}\n")

        ok
      case Delete(keys) =>

        val ok = index.remove(keys)._1

        if(!ok)
        println(s"delete => ${ok}\n")

        ok
    }
  }

  def read(keys: Seq[K]): Seq[(K, Option[V])] = {
    val index = root.get()
    index.read(keys)
  }

  def execute(commands: Seq[Command[T, K, V]]): Boolean = {

    val old = root.get()
    val index = old.copy()

    val max = index.max

    val size = commands.length

    val count = commands.filter(_.isInstanceOf[Add[T, K, V]])
      .map(_.asInstanceOf[Add[T, K, V]].data.length).sum

    if(count > MIN){

      println(s"too many insertions: ${count}!\n")

      return false
    }

    for(i<-0 until size){
      if(!execute(commands(i), index)) {

        println("ooopsss!!")

        return false
      }
    }

    if(meta.isEmpty()){

      if(index.isEmpty()) return true

      return meta.execute(Seq(
        Add(Seq(index.max.get -> this))
      )) && root.compareAndSet(old, index)
    }

    // Fix the method isFull() in Index...
    if(index.overflow()){

      println(s"FULL PARTITION... ${index.size} ${index.MAX}\n")

      val r = index.split()
      var cmds = Seq.empty[Command[T, K, Partition[T, K, V]]]

      val right = new Partition[T, K, V](MIN, MAX, meta)
      right.root.set(r)

      if(max.isDefined){
        cmds = cmds :+ Delete[T, K, Partition[T, K, V]](Seq(max.get))
      }

      cmds = cmds :+ Add[T, K, Partition[T, K, V]](Seq(
        index.max.get -> this,
        r.max.get -> right
      ))

      return meta.execute(cmds) && root.compareAndSet(old, index)
    }

    if(max.isDefined && index.isEmpty()){

      println(s"EMPTY PARTITION...\n")

      return meta.execute(Seq(Delete(Seq(max.get)))) && root.compareAndSet(old, index)
    }

    val nmax = index.max

    if(max.isDefined && nmax.isDefined && !ord.equiv(max.get, nmax.get)){
      return meta.execute(Seq(
        Delete(Seq(max.get)),
        Add(Seq(nmax.get -> this))
      )) && root.compareAndSet(old, index)
    }

    root.compareAndSet(old, index)
  }

  def inOrder(): Seq[(K, V)] = {
    root.get().inOrder()
  }

}
