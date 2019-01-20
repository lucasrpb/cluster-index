package cluster.index

import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

class Block[T: ClassTag, K: ClassTag, V: ClassTag](val id: T,
                                                   val MIN: Int,
                                                   val MAX: Int)(implicit ord: Ordering[K]) {

  val MIDDLE = MIN

  var size = 0
  val keys = Array.ofDim[(K, V)](MAX)

  def find(k: K, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, keys(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insertAt(k: K, v: V, idx: Int): (Boolean, Int) = {
    for(i<-size until idx by -1){
      keys(i) = keys(i - 1)
    }

    keys(idx) = k -> v

    size += 1

    true -> idx
  }

  def read(keys: Seq[K]): Seq[(K, Option[V])] = {
    keys.map{k =>
      val (found, pos) = find(k, 0, size - 1)

      if(!found){
        k -> None
      } else {
        k -> Some(this.keys(pos)._2)
      }
    }
  }

  def insert(k: K, v: V): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val (found, idx) = find(k, 0, size - 1)

    if(found) return false -> 0

    insertAt(k, v, idx)
  }

  def insert(data: Seq[(K, V)]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val len = Math.min(MAX - size, data.length)

    for(i<-0 until len){
      val (k, v) = data(i)
      if(!insert(k, v)._1) return false -> 0
    }

    true -> len
  }

  def removeAt(idx: Int): (K, V) = {
    val data = keys(idx)

    size -= 1

    for(i<-idx until size){
      keys(i) = keys(i + 1)
    }

    data
  }

  def remove(k: K): Boolean = {
    if(isEmpty()) return false

    val (found, idx) = find(k, 0, size - 1)

    if(!found) return false

    removeAt(idx)

    true
  }

  def remove(keys: Seq[K]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    val len = keys.length

    for(i<-0 until len){
      if(!remove(keys(i))) return false -> 0
    }

    true -> len
  }

  def slice(from: Int, n: Int): Seq[(K, V)] = {
    var slice = Seq.empty[(K, V)]

    val len = from + n

    for(i<-from until len){
      slice = slice :+ keys(i)
    }

    for(i<-(from + n) until size){
      keys(i - n) = keys(i)
    }

    size -= n

    slice
  }

  def update(data: Seq[(K, V)]): (Boolean, Int) = {

    val len = data.length

    for(i<-0 until len){
      val (k, v) = data(i)

      val (found, idx) = find(k, 0, size - 1)

      if(!found) return false -> 0

      keys(idx) = k -> v
    }

    true -> len
  }

  def split(): Block[T, K, V] = {
    val right = new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX)

    val len = size
    val middle = len/2

    for(i<-middle until len){
      right.keys(i - middle) = keys(i)

      right.size += 1
      size -= 1
    }

    right
  }

  def copy(): Block[T, K, V] = {
    val copy = new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX)

    copy.size = size

    for(i<-0 until size){
      copy.keys(i) = keys(i)
    }

    copy
  }

  def max: Option[K] = {
    if(isEmpty()) return None
    Some(keys(size - 1)._1)
  }

  def overflow(): Boolean = size >= MIN
  def underflow(): Boolean = size <= MIN/2

  def isFull(): Boolean = size == MAX
  def isEmpty(): Boolean = size == 0

  def canBorrowTo(target: Block[T, K, V]): Boolean = {
    val n = MIN - target.size
    size - MIN >= n
  }

  def borrowRightTo(target: Block[T, K, V]): Block[T, K, V] = {
    val n = MIN - target.size

    val list = slice(0, n)
    target.insert(list)

    target
  }

  def borrowLeftTo(target: Block[T, K, V]): Block[T, K, V] = {
    val n = MIN - target.size

    val list = slice(size - n, n)
    target.insert(list)

    target
  }

  def merge(right: Block[T, K, V]): Block[T, K, V] = {
    val len = right.size
    var j = size

    for(i<-0 until len){
      keys(j) = right.keys(i)
      size += 1
      j += 1
    }

    this
  }

  def inOrder(): Seq[(K, V)] = {
    if(isEmpty()) return Seq.empty[(K, V)]
    keys.slice(0, size)
  }

}