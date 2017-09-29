package PageRank.Spark

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


/**
 * @author ${user.name}
 */
class App{
  
}

object App {
  
  def main(args: Array[String]) {
    // Setting the configuration properties for the program
    val conf = new SparkConf().setAppName("Page Rank").setMaster("local")
    val sc = new SparkContext(conf)

    // Create an RDD from the file read from Input.
    val inputFile = sc.textFile(args(0))

    // Map the input 
    val data = inputFile.map(line => parserNew.parseXML(line)).filter(line => !line.equals(""))
      .map(key => (key.split(" - ")(0).trim(), key.split(" - ")(1)))
      .filter(x => !x._1.trim.equals("")).persist()

    // Identify all dangling nodes in the input.
    val dangling = data.filter(text => !text._2.trim().equals("[]"))
      .map(sub_str => sub_str._2.trim().substring(1, sub_str._2.length() - 1))
      .flatMap(line => line.split(",")).map(x => (x.trim(), 1))
      .reduceByKey((x, y) => x + y)
      .filter(x => x._2 == 1)
      .map(y => (y._1.trim(), "[]"))

    // Create a new RDD with all nodes including dangling nodes.
    val data_with_dangling = (dangling.subtractByKey(data) union data).persist
    
    // Calculate the count of the new RDD.
    var count = data_with_dangling.count
    var pagerank = 1.0 / count
    var ranks = data_with_dangling.mapValues(v => pagerank)
    var i = 0

    // calculate page rank 10 times 
    // Source : This part of the program was referenced and updated 
    //          based on the official spark page rank example program.
    for (i <- 1 to 10) {
      val data_with_ranks = data_with_dangling.join(ranks).values

      val contributions = data_with_ranks.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.substring(1, urls.length() - 1).split(",")
            .filter(x => !x.trim().equals(""))
            .map({
              url => (url.trim(), rank / size)
            })
      }

      // Calculate the contributions to delta. 
      var delta_acc = data_with_ranks
        .filter(x => x._1.trim().equals("[]"))
        .reduceByKey((x, y) => x + y)

      // Calculate delta value to be assigned to each node.
      var delta = delta_acc.first()._2 / count

      var damping_factor = 0.15
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues((damping_factor / count) + 0.85 * _ + delta)
    }

    // Find the top 100 elements 
    val output = ranks.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
    val result = sc.parallelize(output)

    // Save the output file in the directory specified
    result.saveAsTextFile(args(1))
  }

}
