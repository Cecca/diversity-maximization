package it.unipd.dei.diversity.wiki

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

object SubsetSelector {

  def selectSubset(documents: RDD[WikiBagOfWords],
                   query: String,
                   distance: (WikiBagOfWords, WikiBagOfWords) => Double,
                   radius: Double = 1.0)
  : RDD[WikiBagOfWords] = {
    documents.persist(StorageLevel.MEMORY_AND_DISK)
    val center: WikiBagOfWords =
      documents.filter(_.title == query).collect().toSeq match {
        case Seq(bow) => bow
        case Seq() => throw new IllegalArgumentException("The queried page does not exist")
        case seq =>
          println("WARNING: There are multiple pages with the same title")
          println(seq.mkString("\n"))
          seq.head
      }

    documents.filter { bow =>
      distance(bow, center) < radius
    }
  }


  def main(args: Array[String]) {

    val opts = new ScallopConf(args) {
      lazy val dataset = opt[String](required = true)
      lazy val query   = opt[String](required = true)
    }
    opts.verify()

    val inputDataset = opts.dataset()
    val queryString  = opts.query()

    val conf = new SparkConf(loadDefaults = true)
      .setAppName("Test SubsetSelector")
    val sc = new SparkContext(conf)
    val input = CachedDataset(sc, inputDataset)
    val filtered = selectSubset(input, queryString, WikiBagOfWords.cosineDistance)
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(filtered.map(bow => s"${bow.title} :: ${bow.categories}").collect().mkString("\n"))
    println(s"Selected a total of ${filtered.count()} documents over ${input.count()}")

  }

}
