9 lines (87 sloc)  3.45 KB

package Grocery_Itemset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object GroceryItemset {
  
def main(args: Array[String]): Unit = {
  
    val inputDF = spark.read.option("header","true").option("inferSchema","true").csv("file:///input_location/Groceries_dataset.csv")
    val sortDF = inputDF.orderBy(col("Member_number"),col("Date"),col("itemDescription"))
    val collectionDF = sortDF.groupBy(col("Member_number"),col("Date")).agg(collect_set(col("itemDescription")).alias("item"))
    val transactions: List[Itemset] = collectionDF.agg(collect_list(df2.col("item")))
    val frequentItemsets = new NaiveApriori().execute(transactions, 3)
    printItemsets(frequentItemsets)
  }

    val spark = SparkSession
      .builder()
      .appName("Grocery_Itemset")
      .master("local")
      .getOrCreate()
}

class NaiveApriori extends FIM {

  override def findFrequentItemsets(fileName: String, separator: String, transactions: List[Itemset], minSupport: Double): List[Itemset] = {
    val support = Util.absoluteSupport(minSupport, transactions.size)
    val items = findSingletons(transactions, support)
    val frequentItemsets = mutable.Map(1 -> items.map(i => List(i)))

    var k = 1
    while (frequentItemsets.get(k).nonEmpty) {
      k = k + 1
      val candidateKItemsets = findKItemsetsNaive(frequentItemsets(k - 1), k)
      val frequents = filterFrequentItemsets(candidateKItemsets, transactions, support)
      if (frequents.nonEmpty) {
        frequentItemsets.update(k, frequents)
      }
    }
    frequentItemsets.values.flatten.toList
  }

  def filterFrequentItemsets(possibleItemsets: List[Itemset], transactions: List[Itemset], minSupport: Int) = {
    val map = mutable.Map() ++ possibleItemsets.map((_, 0)).toMap
    for (t <- transactions) {
      for ((itemset, count) <- map) {
        // Transactions contains all items from itemset
        if (t.intersect(itemset).size == itemset.size) {
          map.update(itemset, count + 1)
        }
      }
    }
    map.filter(_._2 >= minSupport).keySet.toList
  }

  private def findSingletons(transactions: List[Itemset], minSupport: Int) = {
    transactions.flatten
      .groupBy(identity)
      .map(t => (t._1, t._2.size))
      .filter(_._2 >= minSupport)
      .keySet.toList.sorted
  }

  private def findKItemsetsNaive(items: List[Itemset], n: Int) = {
    val flatItems = items.flatten.distinct
    (new NaiveFIM().subsets(flatItems) :+ flatItems.sorted)
      .filter(_.size == n)
    
    flatItems.coalesce(2). \
        write. \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)
  }
    
}
