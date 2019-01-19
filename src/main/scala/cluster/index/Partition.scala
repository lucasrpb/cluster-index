package cluster.index

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import commands._

import scala.collection.script.Remove
import scala.reflect.ClassTag

class Partition[T: ClassTag, K: ClassTag, V: ClassTag](val MIN: Int,
                                                       val MAX: Int,
                                                       val meta: Meta[T, K, V])(implicit val ord: Ordering[K]) {

  val root = new AtomicReference[Block[T, K, V]](new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T],
    MIN, MAX))

  def execute(cmd: Command[T, K, V], index: Block[T, K, V]): Boolean = {
    cmd match {
      case Insert(data) => index.insert(data)._1
      case Update(data) => index.update(data)._1
      case Delete(keys) => index.remove(keys)._1
    }
  }

  def execute(commands: Seq[Command[T, K, V]]): Boolean = {

    val old = root.get()
    val index = old.copy()

    val max = index.max

    val size = commands.length

    for(i<-0 until size){
      if(!execute(commands(i), index)) return false
    }

    if(meta.isEmpty()){
      return meta.execute(Seq(
        Insert(Seq(index.max.get -> this))
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

      cmds = cmds :+ Insert[T, K, Partition[T, K, V]](Seq(
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
        Insert(Seq(nmax.get -> this))
      )) && root.compareAndSet(old, index)
    }

    root.compareAndSet(old, index)
  }

  def inOrder(): Seq[(K, V)] = {
    root.get().inOrder()
  }

}
