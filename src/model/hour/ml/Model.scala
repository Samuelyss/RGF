package model.hour.ml

import common.SparkCont._
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.RandomForestRegressor

object Model {
  val impurity = "variance"
  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(maxbin)
  val rf = new RandomForestRegressor()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setMaxBins(maxbin)
    .setMaxDepth(MaxDepth) // 9 -- 11; 7:气温融合不够，11:雨量误差奇异点，5雨量--11其它要素会产生震荡，故推荐10
    .setNumTrees(numtrees) //30
    .setMinInstancesPerNode(MinInstancesPerNode)
    .setImpurity(impurity)
    .setFeatureSubsetStrategy("auto")
    .setSeed(5043)
    .setMaxMemoryInMB(MaxMemoryInMB)
    .setCacheNodeIds(true)
    .setCheckpointInterval(CheckpointInterval)
  /*
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxBins(maxbin)
      .setMaxIter(6)
      .setMaxDepth(5)
      .setMinInstancesPerNode(MinInstancesPerNode)
      .setLossType("squared")
      .setMaxMemoryInMB(512)
      .setImpurity(impurity)
      .setSeed(5043)
      .setMaxMemoryInMB(512)
      .setCacheNodeIds(true)
      .setCheckpointInterval(20)
    // Chain indexer and model in a Pipeline
    val pipeline = new Pipeline().setStages(Array(featureIndexer, gbt))
    val model = pipeline.fit(aLearnDfHist)
    mkPredict(aUseDf, model, eleNm)
  }
   */
}
