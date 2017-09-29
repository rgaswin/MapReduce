package PageRank.Spark

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.feature.{ HashingTF, IDF }

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.sql.SQLContext

/**
 * @author ${user.name}
 */

class App {

}

object App {

  /* The source code for this program is mostly inspired and adapted from the official spark documentation :
      https://spark.apache.org/docs/latest/mllib-ensembles.html */
  
  def main(args: Array[String]) {
    //  Setting the configuration properties for the program
    val conf = new SparkConf().setAppName("Data Mining").setMaster("yarn")
    val sc = new SparkContext(conf)
   

    // Read Unlabeled Data from the input.
    val unlabeledData = sc.textFile(args(0))

    // Load the saved model from S3
    val model = RandomForestModel.load(sc, args(1))
    
    // Get the header of the data
    val unlabeledHeader = unlabeledData.first()

    // Get data without the header and also get only rows who have more than 27 columns   
    val dataWithoutHeader = unlabeledData.filter(row => row != unlabeledHeader)
      .filter(row => row.split(',').length >= 27)

    // Isolate and select a particular set of columns
    val prunedData = dataWithoutHeader.map(line => {
      val lines = line.split(',')
      (
        (lines(0), Array[Double](
          if (lines(2) == "?" || lines(2) == "X") 0 else try { (lines(2).toDouble) } catch { case _ => 0 },
          if (lines(3) == "?" || lines(3) == "X") 0 else try { (lines(3).toDouble) } catch { case _ => 0 },
          if (lines(4) == "?" || lines(4) == "X") 0 else try { (lines(4).toDouble) } catch { case _ => 0 },
          if (lines(5) == "?" || lines(5) == "X") 0 else try { (lines(5).toDouble) } catch { case _ => 0 },
          if (lines(6) == "?" || lines(6) == "X") 0 else try { (lines(6).toDouble) } catch { case _ => 0 },
          if (lines(7) == "?" || lines(7) == "X") 0 else try { (lines(7).toDouble) } catch { case _ => 0 },
          if (lines(13) == "?" || lines(13) == "X") 0 else try { (lines(13).toDouble) } catch { case _ => 0 },
          if (lines(16) == "?" || lines(16) == "X") 0 else try { (lines(16).toDouble) } catch { case _ => 0 },
          if (lines(18) == "?" || lines(18) == "X") 0 else try { (lines(18).toDouble) } catch { case _ => 0 },
          if (lines(19) == "?" || lines(19) == "X") 0 else try { (lines(19).toDouble) } catch { case _ => 0 },
          if (lines(20) == "?" || lines(20) == "X") 0 else try { (lines(20).toDouble) } catch { case _ => 0 },
          if (lines(956) == "?" || lines(956) == "X") 0 else try { (lines(956).toDouble) } catch { case _ => 0 },
          if (lines(959) == "?" || lines(959) == "X") 0 else try { (lines(959).toDouble) } catch { case _ => 0 },
          if (lines(966) == "?" || lines(966) == "X") 0 else try { (lines(966).toDouble) } catch { case _ => 0 },
          if (lines(967) == "?" || lines(967) == "X") 0 else try { (lines(967).toDouble) } catch { case _ => 0 },

          // 1020 - 1031

          if (lines(1020) == "?" || lines(1020) == "X") 0 else try { (lines(1020).toDouble) } catch { case _ => 0 },
          if (lines(1021) == "?" || lines(1021) == "X") 0 else try { (lines(1021).toDouble) } catch { case _ => 0 },
          if (lines(1022) == "?" || lines(1022) == "X") 0 else try { (lines(1022).toDouble) } catch { case _ => 0 },
          if (lines(1023) == "?" || lines(1023) == "X") 0 else try { (lines(1023).toDouble) } catch { case _ => 0 },
          if (lines(1024) == "?" || lines(1024) == "X") 0 else try { (lines(1024).toDouble) } catch { case _ => 0 },
          if (lines(1025) == "?" || lines(1025) == "X") 0 else try { (lines(1025).toDouble) } catch { case _ => 0 },
          if (lines(1026) == "?" || lines(1026) == "X") 0 else try { (lines(1026).toDouble) } catch { case _ => 0 },
          if (lines(1027) == "?" || lines(1027) == "X") 0 else try { (lines(1027).toDouble) } catch { case _ => 0 },
          if (lines(1028) == "?" || lines(1028) == "X") 0 else try { (lines(1028).toDouble) } catch { case _ => 0 },
          if (lines(1029) == "?" || lines(1029) == "X") 0 else try { (lines(1029).toDouble) } catch { case _ => 0 },
          if (lines(1030) == "?" || lines(1030) == "X") 0 else try { (lines(1030).toDouble) } catch { case _ => 0 },
          if (lines(1031) == "?" || lines(1031) == "X") 0 else try { (lines(1031).toDouble) } catch { case _ => 0 },

          // 1058 -1069

          if (lines(1058) == "?" || lines(1058) == "X") 0 else try { (lines(1058).toDouble) } catch { case _ => 0 },
          if (lines(1059) == "?" || lines(1059) == "X") 0 else try { (lines(1059).toDouble) } catch { case _ => 0 },
          if (lines(1060) == "?" || lines(1060) == "X") 0 else try { (lines(1060).toDouble) } catch { case _ => 0 },
          if (lines(1061) == "?" || lines(1061) == "X") 0 else try { (lines(1061).toDouble) } catch { case _ => 0 },
          if (lines(1062) == "?" || lines(1062) == "X") 0 else try { (lines(1062).toDouble) } catch { case _ => 0 },
          if (lines(1063) == "?" || lines(1063) == "X") 0 else try { (lines(1063).toDouble) } catch { case _ => 0 },
          if (lines(1064) == "?" || lines(1064) == "X") 0 else try { (lines(1064).toDouble) } catch { case _ => 0 },
          if (lines(1065) == "?" || lines(1065) == "X") 0 else try { (lines(1065).toDouble) } catch { case _ => 0 },
          if (lines(1066) == "?" || lines(1066) == "X") 0 else try { (lines(1066).toDouble) } catch { case _ => 0 },
          if (lines(1067) == "?" || lines(1067) == "X") 0 else try { (lines(1067).toDouble) } catch { case _ => 0 },
          if (lines(1068) == "?" || lines(1068) == "X") 0 else try { (lines(1068).toDouble) } catch { case _ => 0 },
          if (lines(1069) == "?" || lines(1069) == "X") 0 else try { (lines(1069).toDouble) } catch { case _ => 0 },
          if (lines(26) == "?" || lines(26) == "X") 0 else try { (lines(26).toDouble) } catch { case _ => 0 })))
    }).persist()

    // Create labeled points from the prunedData
    val labeledPoints = prunedData.map { line =>
      val parts = line._2
      (line._1, LabeledPoint(if (parts(39) == 0) 0 else 1, Vectors.dense(parts.init)))
    }

    // Predict occurance based on model trained already
    val finalPrediction = labeledPoints.map { point =>
      val prediction = model.predict(point._2.features)
      (point._1, prediction)
    }

    // Output as commma separated values
    finalPrediction.map(a => a._1 + "," + a._2.toString()).saveAsTextFile(args(2))

  }
}