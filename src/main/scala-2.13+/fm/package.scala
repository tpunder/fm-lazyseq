package fm

package object lazyseq {
  type Growable[-A] = scala.collection.mutable.Growable[A]
}