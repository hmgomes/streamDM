/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.streamdm.classifiers.trees

import scala.collection.mutable.HashSet
import scala.math.{sqrt, log => math_log}
import scala.collection.mutable.Queue
import org.apache.spark.internal.Logging
import com.github.javacliparser._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf, window}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream._
import org.apache.spark.streamdm.utils.Utils.{argmax, normal}
import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.classifiers._
import org.apache.spark.streamdm.core.specification.ExampleSpecification
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener}
import org.apache.spark.sql.execution.streaming.Sink

import org.apache.spark.sql.Encoders._

/**
 *
 * The Hoeffding tree is an incremental decision tree learner for large data
 * streams, that assumes that the data distribution is not changing over time.
 * It grows incrementally a decision tree based on the theoretical guarantees of
 * the Hoeffding bound (or additive Chernoff bound). A node is expanded as soon
 * as there is sufficient statistical evidence that an optimal splitting feature
 * exists, a decision based on the distribution-independent Hoeffding bound. The
 * model learned by the Hoeffding tree is asymptotically nearly identical to the
 * one built by a non-incremental learner, if the number of training instances
 * is large enough.
 *
 * <p>It is controlled by the following options:
 * <ul>
 * <li> numeric observer to use (<b>-n</b>); for the moment, only Gaussian
 * approximation is supported; class of type FeatureClassObserver;
 * <li> number of examples a leaf should observe before a split attempt
 * (<b>-g</b>);
 * <li> Number of examples a leaf should observe before applying NaiveBayes
 * (<b>-q</b>);
 * <li> split criterion to use (<b>-s</b>), an object of type SplitCriterionType;
 * <li> allowable error in split decision (<b>-c</b>);
 * <li> threshold of allowable error in breaking ties (<b>-t</b>);
 * <li> allow only binary splits (<b>-b</b>), boolean flag;
 * <li> disable poor attributes (<b>-r</b>);
 * <li> allow growth (<b>-o</b>);
 * <li> disable pre-pruning (<b>-p</b>);
 * <li> leaf prediction to use (<b>-l</b>), either MajorityClass (0), NaiveBayes
 * (1) or adaptive NaiveBayes (2, default);
 * <li> enable splitting at all leaves (<b>-a</b>); the original algorithm can
 * split at any time, but in Spark Streaming one can only split once per RDD; the option
 * controls whether to split at leaf of the last Example of the RDD, or at every leaf.
 * </ul>
 */

//case class HTModelState(model: HoeffdingTreeModel)
case class DummyEventObject(dummy: Int)
case class InputRow(X: Vector, y: Int)

//case class InputRow(classLabel: Int)


class HoeffdingTree extends Classifier with Logging {

  type T = HoeffdingTreeModel

  val numericObserverTypeOption: IntOption = new IntOption("numericObserverType", 'n',
    "numeric observer type, 0: gaussian", 0, 0, 2)

  val splitCriterionOption: ClassOption = new ClassOption("splitCriterion", 's',
    "Split criterion to use.", classOf[SplitCriterion], "InfoGainSplitCriterion")

  val growthAllowedOption: FlagOption = new FlagOption("growthAllowed", 'o',
    "Allow to grow")

  val binaryOnlyOption: FlagOption = new FlagOption("binaryOnly", 'b',
    "Only allow binary splits")

  val numGraceOption: IntOption = new IntOption("numGrace", 'g',
    "The number of examples a leaf should observe between split attempts.",
    200, 1, Int.MaxValue)

  val tieThresholdOption: FloatOption = new FloatOption("tieThreshold", 't',
    "Threshold below which a split will be forced to break ties.", 0.05, 0, 1)

  val splitConfidenceOption: FloatOption = new FloatOption("splitConfidence", 'c',
    "The allowable error in split decision, values closer to 0 will take longer to decide.",
    0.0000001, 0.0, 1.0)

  val learningNodeOption: IntOption = new IntOption("learningNodeType", 'l',
    "Learning node type of leaf", 0, 0, 2)

  val nbThresholdOption: IntOption = new IntOption("nbThreshold", 'q',
    "The number of examples a leaf should observe between permitting Naive Bayes", 0, 0, Int.MaxValue)

  val noPrePruneOption: FlagOption = new FlagOption("noPrePrune", 'p', "Disable pre-pruning.")

  val removePoorFeaturesOption: FlagOption = new FlagOption("removePoorFeatures", 'r', "Disable poor features.")

  val splitAllOption: FlagOption = new FlagOption("SplitAll", 'a', "Split at all leaves")

  val maxDepthOption: IntOption = new IntOption("MaxDepth", 'h',
    "The max depth of tree to stop growing",
    20, 0, Int.MaxValue)

  var schema: StructType = null

//  var model: HoeffdingTreeModel = null

  /* Init the model used for the Learner*/
  override def init(exampleSpecification: ExampleSpecification): Unit = ???
//  {
//    espec = exampleSpecification
//    val numFeatures = espec.numberInputFeatures()
//    val outputSpec = espec.outputFeatureSpecification(0)
//    val numClasses = outputSpec.range()
//    model = new HoeffdingTreeModel(espec, numericObserverTypeOption.getValue, splitCriterionOption.getValue(),
//      true, binaryOnlyOption.isSet(), numGraceOption.getValue(),
//      tieThresholdOption.getValue, splitConfidenceOption.getValue(),
//      learningNodeOption.getValue(), nbThresholdOption.getValue(),
//      noPrePruneOption.isSet(), removePoorFeaturesOption.isSet(), splitAllOption.isSet(),
//      maxDepthOption.getValue())
//    model.init()
//  }

  override def init(schema: StructType): Unit = {
    this.schema = schema
    val numFeatures = schema.fields.length - 1 // Ignore the class field
    val numClasses = this.schema("class").metadata.getLong("num_class").toInt

    HoeffdingTreeLearnerObject.model  = new HoeffdingTreeModel(this.schema, numericObserverTypeOption.getValue, splitCriterionOption.getValue(),
          true, binaryOnlyOption.isSet(), numGraceOption.getValue(),
          tieThresholdOption.getValue, splitConfidenceOption.getValue(),
          learningNodeOption.getValue(), nbThresholdOption.getValue(),
          noPrePruneOption.isSet(), removePoorFeaturesOption.isSet(), splitAllOption.isSet(),
          maxDepthOption.getValue())

    HoeffdingTreeLearnerObject.model.init()
  }

  /* Gets the current model used for the Learner.
   * 
   * @return the Model object used for training
   */
  override def getModel: HoeffdingTreeModel = HoeffdingTreeLearnerObject.model

  /* Train the model from the stream of instances given for training.
   *
   * @param input a stream of instances
   * @return Unit
   */
  def train(input: DStream[Example]): Unit = ???
  //    {
  //      input.foreachRDD {
  //      rdd =>
  //        val tmodel = rdd.aggregate(
  //          new HoeffdingTreeModel(model))(
  //            (mod, example) => { mod.update(example) },
  //          (mod1, mod2) => { mod1.merge(mod2, false) })
  //        model = model.merge(tmodel, true)
  //    }

  override def train(stream: DataFrame): Unit = {
    logInfo("train invoked")

    import stream.sparkSession.implicits._

//    implicit val htModelEncoder = Encoders.kryo[HoeffdingTreeModel]

    val query = stream
      .selectExpr("X", "class as y").as[InputRow]

      .groupByKey( _.y ) // inputRow.row.getInt( inputRow.row.schema.fieldIndex("class") )) //(window($"processingTime", "20 seconds", "5 seconds"))
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)

      .writeStream
      .queryName("train-query")
      .format("console")
      .outputMode("update")
      .start()

//      .outputMode("update")
//      .format("console")
//      .start()

    query.awaitTermination()

//    val query = stream
//      .writeStream
//      .queryName("train-query")
//      .outputMode("update")
//      .option("checkpointLocation", "../data/tmp/train-checkpoint")
//      .format("org.apache.spark.streamdm.classifiers.trees.HoeffdingTreeTrainSinkProvider")
//      .start()



//    val writer = new TrainingWriter(new HTModelState(1))
//
////    this.model.merge(writer.executorModel, true)
//
//    println("this.model.description in train(stream: DataFrame) = " + this.model.description())
//
//    val query = stream
//      .writeStream
//      .queryName("train-query")
//      .foreach(writer)

//      .writeStream
//      .queryName("training-query")
//      .foreach(writer)
//      .start()
  }


//  class trainingWriter(val model: HoeffdingTreeModel) extends ForeachWriter[Row] {
//
//    // This is done in the driver, thus we copy the original model to all executors...
//    val executorModel: HoeffdingTreeModel = model
//
//    override def open(partitionId: Long, version: Long): Boolean = true
//    override def process(row: Row) = {
//      val indexOfX: Int = row.schema.fieldIndex("X")
//      val vector:Vector = row(indexOfX).asInstanceOf[Vector]
//
//      println("[EXECUTOR CODE] Field index of X = %d and value = %s".format(indexOfX, vector.toString()))
//    }
//    override def close(errorOrNull: Throwable) = {}
//  }


  override def predict(stream: DataFrame): DataFrame = {
    // User defined function (UDF) to apply the model.predict to the column X from stream DataFrame
    val predictionsUDF = udf { (features: Vector) =>
      HoeffdingTreeLearnerObject.model.predict(features)
    }
    val streamWithPredictions = stream.withColumn("predicted", predictionsUDF(stream.col("X")))
    streamWithPredictions
  }


  def updateAcrossEvents(processingTime: Int,
                          rows: Iterator[InputRow],
                         oldState: GroupState[DummyEventObject]): DummyEventObject = {

    logInfo("updateAcrossEvents")

    val currentModel = if(oldState.exists) oldState.get else DummyEventObject(1)

    if(!oldState.exists) {
      HoeffdingTreeLearnerObject.model = new HoeffdingTreeModel(this.schema, numericObserverTypeOption.getValue, splitCriterionOption.getValue(),
        true, binaryOnlyOption.isSet(), numGraceOption.getValue(),
        tieThresholdOption.getValue, splitConfidenceOption.getValue(),
        learningNodeOption.getValue(), nbThresholdOption.getValue(),
        noPrePruneOption.isSet(), removePoorFeaturesOption.isSet(), splitAllOption.isSet(),
        maxDepthOption.getValue())

      HoeffdingTreeLearnerObject.model.init()
    }

    for(input <- rows) {
      HoeffdingTreeLearnerObject.model.update(input.X, input.y, 1.0)
      oldState.update(new DummyEventObject(currentModel.dummy+1))
    }

    currentModel
  }

  // TODO: Remove
  def predict(input: DStream[Example]): DStream[(Example, Double)] = ???
}

/***
  * The HoeffdingTree object used to hold the model
  */
object HoeffdingTreeLearnerObject {
  var model: HoeffdingTreeModel = null
}

class HoeffdingTreeModel(val schema: StructType, val numericObserverType: Int = 0,
  val splitCriterion: SplitCriterion = new InfoGainSplitCriterion(),
  var growthAllowed: Boolean = true, val binaryOnly: Boolean = true,
  val graceNum: Int = 200, val tieThreshold: Double = 0.05,
  val splitConfedence: Double = 0.0000001, val learningNodeType: Int = 0,
  val nbThreshold: Int = 0, val noPrePrune: Boolean = true,
  val removePoorFeatures: Boolean = false, val splitAll: Boolean = false, val maxDepth: Int = 20)
    extends ClassificationModel with Serializable with Logging {

  type T = HoeffdingTreeModel

  val numFeatures = schema.fields.length - 1 // Ignore the class field
  val numClasses = this.schema("class").metadata.getLong("num_class").toInt

  var activeNodeCount: Int = 0
  var inactiveNodeCount: Int = 0
  var deactiveNodeCount: Int = 0
  var decisionNodeCount: Int = 0

  var baseNumExamples: Int = 0
  var blockNumExamples: Int = 0

  var root: Node = null

  def this(model: HoeffdingTreeModel) {
    this(model.schema, model.numericObserverType, model.splitCriterion, model.growthAllowed,
      model.binaryOnly, model.graceNum, model.tieThreshold, model.splitConfedence,
      model.learningNodeType, model.nbThreshold, model.noPrePrune)
    activeNodeCount = model.activeNodeCount
    this.inactiveNodeCount = model.inactiveNodeCount
    this.deactiveNodeCount = model.deactiveNodeCount
    this.decisionNodeCount = model.decisionNodeCount
    baseNumExamples = model.baseNumExamples + model.blockNumExamples
    this.root = model.root

//    for (i <- 0 until this.root.blockClassDistribution.length)
//      this.root.blockClassDistribution(i) += model.root.blockClassDistribution(i)
  }

  /* init the model */
  def init(): Unit = {
    //create an ActiveLearningNode for root
    root = createLearningNode(learningNodeType, numClasses)
    activeNodeCount += 1
  }

  /* Update the model from the stream of instances given for training.
   *
   * @param input an example instance
   * @return current model
   */
  override def update(example: Example): HoeffdingTreeModel = ???
//  {
//    blockNumExamples += 1
//
//    if(blockNumExamples % graceNum == 1){
//      listExamples.append(example)
//    }
//    lastExample = example
//    if (root == null) {
//      init
//    }
//    // filter the example instance to the responding FoundNode
//    val foundNode = root.filterToLeaf(example, null, -1)
//    var leafNode = foundNode.node
//
//    if (leafNode.isInstanceOf[LearningNode]) {
//      val learnNode = leafNode.asInstanceOf[LearningNode]
//      //update the learning node
//      learnNode.learn(this, example)
//    }
//    this
//  }

  override def update(vector: Vector, classLabel: Int, weight: Double): HoeffdingTreeModel = {
    blockNumExamples += 1

    logInfo("!!!!! Invoked UPDATE !!!!!!")

//    println(s"blockNumExamples = ($blockNumExamples )")

    if (root == null) {
      init
    }
    val foundNode = root.filterToLeaf(vector, null, -1)
    val leafNode = foundNode.node

    if (leafNode.isInstanceOf[LearningNode]) {
      val learnNode = leafNode.asInstanceOf[LearningNode]
//      learnNode.learn(this, vector, classLabel, weight)

learnNode.classDistribution(classLabel) += 1 // TODO: THIS IS SANITY CHECK CODE, MUST BE CHANGED
    }

    this
  }

  /* try to split the learning node
   * 
   * @param learnNode the node which may be splitted
   * @param parent parent of the learnNode
   * @param pIndex learnNode's index of the parent
   * @return Unit 
   */
  def attemptToSplit(learnNode: LearningNode, parent: SplitNode, pIndex: Int): Unit = {
    if (growthAllowed && learnNode.isInstanceOf[ActiveLearningNode]) {
      // split only happened when the tree is allowed to grow and the node is a ActiveLearningNode
      val activeNode = learnNode.asInstanceOf[ActiveLearningNode]
      val isPure = activeNode.isPure()
      if (!isPure) {
        // one best suggestion for each feature
        var bestSuggestions: Array[FeatureSplit] = activeNode.getBestSplitSuggestions(splitCriterion, this)
        //sort the suggestion based on the merit
        bestSuggestions = bestSuggestions.sorted
        if (shouldSplit(activeNode, bestSuggestions)) {
          val best: FeatureSplit = bestSuggestions.last
          if (best.conditionalTest == null) {
            //deactivate a learning node
            deactiveLearningNode(activeNode, parent, pIndex)
          } else {
            logInfo("split! ")
            //replace the ActiveLearningNode with a SplitNode and create children
            val splitNode: SplitNode = new SplitNode(activeNode.classDistribution, best.conditionalTest)
            for (index <- 0 until best.numSplit) {
              splitNode.setChild(index,
                createLearningNode(learningNodeType, best.distributionFromSplit(index)))
            }
            // replace the node
            addSplitNode(splitNode, parent, pIndex)
          }
          val tree_size_nodes = activeNodeCount + decisionNodeCount + inactiveNodeCount
          val tree_size_leaves = activeNodeCount + inactiveNodeCount
          logInfo("{" + tree_size_nodes + "," + tree_size_leaves + "," + activeNodeCount + "," + treeHeight() + "}")

          }
          // todo manage memory
        }
    }
  }

  /**
   * check whether split the activeNode or not according to Heoffding bound and merit
   * @param activeNode the node which may be splitted
   * @param bestSuggestions array of FeatureSplit
   * @return Boolean
   */
  def shouldSplit(activeNode: ActiveLearningNode, bestSuggestions: Array[FeatureSplit]): Boolean = {
    if (bestSuggestions.length < 2) {
      bestSuggestions.length > 0
    } else {
      val hoeffdingBound = computeHoeffdingBound(activeNode)
      val length = bestSuggestions.length
      if (hoeffdingBound < tieThreshold ||
        bestSuggestions.last.merit - bestSuggestions(length - 2).merit > hoeffdingBound) {
        true
      } else false
    }
  }

  def disableFeatures(activeNode: ActiveLearningNode, bestSuggestions: Array[FeatureSplit], hoeffdingBound: Double): Unit = {
    if (this.removePoorFeatures) {
      val poorFeatures = new HashSet[Integer]()
      val bestSuggestion = bestSuggestions.last
      for (suggestion: FeatureSplit <- bestSuggestions) {
        if (suggestion.conditionalTest != null &&
          bestSuggestion.merit - suggestion.merit > hoeffdingBound) {
          val fIndex = suggestion.conditionalTest.featureIndex()
          poorFeatures.add(fIndex)
          activeNode.disableFeature(fIndex)
        }
      }
    }
  }

  /**
    * Merge function: merge with another model's FeatureObservers and root, and try to split
    * @param that : other HoeffdingTree Model
    * @param trySplit: (false: only update statistics, true: attempt to split).
    * @return this
    */
  def merge(that: HoeffdingTreeModel, trySplit: Boolean): HoeffdingTreeModel = {

    println("First merge on the HoeffdingTreeModel class")
    // merge root with another root
    root.merge(that.root, trySplit)

    if (trySplit) {
      if(this.treeHeight() < maxDepth){

          val queue = new Queue[FoundNode]()
          queue.enqueue(new FoundNode(root, null, -1))
          var numLeaves = 0
          var toSplit = 0
          var totalAddOnWeight = 0.0
          while (queue.size > 0) {
            val foundNode = queue.dequeue()
            foundNode.node match {
              case splitNode: SplitNode => {
                for (i <- 0 until splitNode.children.length)
                  queue.enqueue(new FoundNode(splitNode.children(i), splitNode, i))
              }
              case activeNode: ActiveLearningNode => {
                totalAddOnWeight = totalAddOnWeight+ activeNode.addOnWeight()
                numLeaves = numLeaves + 1
                if(activeNode.addOnWeight() > graceNum){
                  attemptToSplit(activeNode, foundNode.parent, foundNode.index)
                  toSplit = toSplit + 1
                }
              }
              case _: Node => {}
            }
          }
        }
      else{
        logInfo("Tree's height exceeds maxDepth")
      }
    }
    this
  }

  /* predict the class of example */
  def predict(example: Example): Double = ???
//  {
//    if (root != null) {
//      val foundNode = root.filterToLeaf(example, null, -1)
//      var leafNode = foundNode.node
//      if (leafNode == null) {
//        leafNode = foundNode.parent
//      }
//      argmax(leafNode.classVotes(this, example))
//    } else {
//      0.0
//    }
//  }

  def predict(vector: Vector): Double = {
    val r = new scala.util.Random
    r.nextInt(2)

//    if(this.root != null) {
//      val foundNode = root.filterToLeaf(vector, null, -1)
//      var leafNode = foundNode.node
//      if (leafNode == null) {
//          leafNode = foundNode.parent
//        }
//        argmax(leafNode.classVotes(this, vector))
//      } else {
//        0.0
//      }
  }

  /* create a new ActiveLearningNode
 * 
 * @param nodeType, (0,ActiveLearningNode),(1,LearningNodeNB),(2,LearningNodeNBAdaptive)
 * @param classDistribution, the classDistribution to init node
 * @return a new LearningNode
 */
  def createLearningNode(nodeType: Int, classDistribution: Array[Double]): LearningNode = nodeType match {
    case 0 => new ActiveLearningNode(classDistribution, this.schema)
    case 1 => new LearningNodeNB(classDistribution, this.schema)
    case 2 => new LearningNodeNBAdaptive(classDistribution, this.schema)
    case _ => new ActiveLearningNode(classDistribution, this.schema)
  }

  /* create a new ActiveLearningNode
 * 
 * @param nodeType, (0,ActiveLearningNode),(1,LearningNodeNB),(2,LearningNodeNBAdaptive)
 * @param numClasses, the number of the classes 
 * @return a new LearningNode
 */
  def createLearningNode(nodeType: Int, numClasses: Int): LearningNode = nodeType match {
    case 0 => new ActiveLearningNode(new Array[Double](numClasses), this.schema)
    case 1 => new LearningNodeNB(new Array[Double](numClasses), this.schema)
    case 2 => new LearningNodeNBAdaptive(new Array[Double](numClasses), this.schema)
    case _ => new ActiveLearningNode(new Array[Double](numClasses), this.schema)
  }

  /* create a new ActiveLearningNode with another LearningNode
 * 
 * @param nodeType, (0,ActiveLearningNode),(1,LearningNodeNB),(2,LearningNodeNBAdaptive)
 * @param that, a default LearningNode to init the LearningNode 
 * @return a new LearningNode
 */
  def createLearningNode(nodeType: Int, that: LearningNode): LearningNode = nodeType match {
    case 0 => new ActiveLearningNode(that.asInstanceOf[ActiveLearningNode])
    case 1 => new LearningNodeNB(that.asInstanceOf[LearningNodeNB])
    case 2 => new LearningNodeNBAdaptive(that.asInstanceOf[LearningNodeNBAdaptive])
  }

  /* repalce an InactiveLearningNode with an ActiveLearningNode
  * 
  * @paren inactiveNode which will be repalced
  * @param parent parent of the node which will be replaced
  * @param pIndex the index of node
  * @return Unit 
  */
  def activeLearningNode(inactiveNode: InactiveLearningNode, parent: SplitNode, pIndex: Int): Unit = {
    val activeNode = createLearningNode(learningNodeType, inactiveNode.classDistribution)
    if (parent == null) {
      root = activeNode
    } else {
      parent.setChild(pIndex, activeNode)
    }
    activeNodeCount += 1
    inactiveNodeCount -= 1
  }

  /* replace an ActiveLearningNode with an InactiveLearningNode
   * @param parent parent of the node which will be replaced
   * @param pIndex the index of node
   * @return Unit
   */
  def deactiveLearningNode(activeNode: ActiveLearningNode, parent: SplitNode, pIndex: Int): Unit = {
    val deactiveNode = new InactiveLearningNode(activeNode.classDistribution)
    if (parent == null) {
      root = deactiveNode
    } else {
      parent.setChild(pIndex, deactiveNode)
    }
    activeNodeCount -= 1
    inactiveNodeCount += 1

  }

  /*
   * replace a activeNode with the splitNode
   * @param splitNode the new SplitNode
   * @param parent parent of the node which will be replaced
   * @param pIndex the index of node 
   * @return Unit 
   */
  def addSplitNode(splitNode: SplitNode, parent: SplitNode, pIndex: Int): Unit = {
    if (parent == null) {
      root = splitNode
    } else {
      parent.setChild(pIndex, splitNode)
    }
    activeNodeCount += splitNode.numChildren() - 1
    decisionNodeCount += 1
  }

  /* Computes Heoffding Bound withe activeNode's class distribution
   * @param activeNode 
   * @return double value
   */
  def computeHoeffdingBound(activeNode: ActiveLearningNode): Double = {
    val rangeMerit = splitCriterion.rangeMerit(activeNode.classDistribution)
    val heoffdingBound = sqrt(rangeMerit * rangeMerit * math_log(1.0 / this.splitConfedence)
      / (activeNode.weight() * 2))
    heoffdingBound
  }
  /*
   * Returns the height of Hoeffding Tree
   */
  def treeHeight(): Int = {
    if (root == null) -1
    else root.height()
  }

  /* Description of the Hoeffding Tree Model
   * 
   * @return a multi-line String
   */
  def description(): String = {
    "Hoeffding Tree Model description:\n" + root.description()
  }

  /** Computes the probability for a given label class, given the current Model
    *
    * @param example the Example which needs a class predicted
    * @return the predicted probability
    */
  override def prob(example: Example): Double = {
    if (root != null) {
      val foundNode = root.filterToLeaf(example, null, -1)
      var leafNode = foundNode.node
      if (leafNode == null) {
        leafNode = foundNode.parent
      }
      argmax(normal(leafNode.classVotes(this, example)))
    } else {
      0.0
    }
  }
}




case class TrainSink (
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, stream: DataFrame): Unit = {
    println(s"TrainSink.addBatch($batchId)")
    //        stream.explain()

//    val microbatch = stream.sparkSession.createDataFrame(
//      stream.sparkSession.sparkContext.parallelize(stream.collect()), stream.schema)

//    println("How many instances are in the microbatch now = " + microbatch.count())

    // Create the executor copy
//    val executorModel: HoeffdingTreeModel = new HoeffdingTreeModel(HoeffdingTreeLearnerObject.model)
//
//    stream.foreach(row => {
//      val indexOfX: Int = row.schema.fieldIndex("X")
//      val indexOfClass: Int = row.schema.fieldIndex("class")
//      val vector:Vector = row(indexOfX).asInstanceOf[Vector]
//      val classLabel: Int = row(indexOfClass).asInstanceOf[Int]
//
//      // TODO: Weight hard coded to 1.0, might not be ideal for ensembling methods that manipulate it.
//      executorModel.update(vector, classLabel, 1.0)
//    } )
//
//    HoeffdingTreeLearnerObject.model.merge(executorModel, true)

//    println("Accessing the model from HoeffdingTreeLearnerObject in the Sink = ")
//    println(HoeffdingTreeLearnerObject.model.description())
//    println(s"\tBlockNumExamples = " + HoeffdingTreeLearnerObject.model.blockNumExamples)
//    microbatch.show(20)
  }


}


class HoeffdingTreeTrainSinkProvider extends StreamSinkProvider
  with DataSourceRegister {

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    TrainSink(
      sqlContext,
      parameters,
      partitionColumns,
      outputMode)
  }

  override def shortName(): String = "hoeffdingtree-train"
}
