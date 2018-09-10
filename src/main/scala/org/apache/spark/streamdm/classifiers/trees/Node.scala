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

import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer
import scala.math.max
import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.classifiers.bayes._
import org.apache.spark.streamdm.utils.Utils.argmax
import org.apache.spark.ml.linalg.Vector

/**
 * Abstract class containing the node information for the Hoeffding trees.
 */
abstract class Node(val classDistribution: Array[Double]) extends Serializable {

  var nodeDepth: Int = 0
  // stores class distribution of a block of RDD
  val blockClassDistribution: Array[Double] = new Array[Double](classDistribution.length)

  /**
    * Filter the data to the related leaf node
    *
    * @param vector the input instance
    * @param parent the parent of current node
    * @param index the index of current node in the parent children
    * @return a FoundNode containing the leaf node
    */
  def filterToLeaf(vector: Vector, parent: SplitNode, index: Int): FoundNode

  /**
    * Return the class distribution
    * @return an Array containing the class distribution
    */
  def classVotes(ht: HoeffdingTreeModel, vector: Vector): Array[Double] =
    classDistribution.clone()

  /**
   * Checks whether a node is a leaf
   * @return <i>true</i> if a node is a leaf, <i>false</i> otherwise
   */
  def isLeaf(): Boolean = true

  /**
   * Returns height of the tree
   *
   * @return the height
   */
  def height(): Int = 0

  /**
   * Returns depth of current node in the tree
   *
   * @return the depth
   */
  def depth(): Int = nodeDepth

  /**
   * Set the depth of current node
   *
   * @param depth the new depth
   */
  def setDepth(depth: Int): Unit = {
    nodeDepth = depth
    if (this.isInstanceOf[SplitNode]) {
      val splitNode = this.asInstanceOf[SplitNode]
      splitNode.children.foreach { _.setDepth(depth + 1) }
    }
  }

  /**
   * Merge two nodes
   *
   * @param that the node which will be merged
   * @param trySplit flag indicating whether the node will be split
   * @return new node
   */
  def merge(that: Node, trySplit: Boolean): Node

  /**
   * Returns number of children
   *
   * @return number of children
   */
  def numChildren(): Int = 0

  /**
   * Returns the node description
   * @return String containing the description
   */
  def description(): String = {
    "  " * nodeDepth + "depth: "+ nodeDepth +" | Leaf" + " weight = " +
      Utils.arraytoString(classDistribution) + "\n"
  }

}

/**
 * The container of a node.
 */
class FoundNode(val node: Node, val parent: SplitNode, val index: Int) extends Serializable {

}

/**
 * Branch node of the Hoeffding tree.
 */
class SplitNode(classDistribution: Array[Double], val conditionalTest: ConditionalTest)
    extends Node(classDistribution) with Serializable {

  val children: ArrayBuffer[Node] = new ArrayBuffer[Node]()

  def this(that: SplitNode) {
    this(Utils.addArrays(that.classDistribution, that.blockClassDistribution),
      that.conditionalTest)
  }

  /**
    * Filter the data to the related leaf node
    *
    * @param vector input instance
    * @param parent the parent of current node
    * @param index the index of current node in the parent children
    * @return FoundNode cotaining the leaf node
    */
  override def filterToLeaf(vector: Vector, parent: SplitNode, index: Int): FoundNode = {
    val cIndex = childIndex(vector)
    if (cIndex >= 0) {
      if (cIndex < children.length && children(cIndex) != null) {
        children(cIndex).filterToLeaf(vector, this, cIndex)
      } else new FoundNode(null, this, cIndex)
    } else new FoundNode(this, parent, index)
  }

  def childIndex(vector: Vector): Int = {
    conditionalTest.branch(vector)
  }

  def setChild(index: Int, node: Node): Unit = {
    if (children.length > index) {
      children(index) = node
      node.setDepth(nodeDepth + 1)
    } else if (children.length == index) {
      children.append(node)
      node.setDepth(nodeDepth + 1)
    } else {
      assert(children.length < index)
    }
  }

  /**
   * Returns whether a node is a leaf
   */
  override def isLeaf() = false

  /**
   * Returns height of the tree
   *
   * @return the height
   */
  override def height(): Int = {
    var height = 0
    for (child: Node <- children) {
      height = max(height, child.height()) + 1
    }
    height
  }

  /**
   * Returns number of children
   *
   * @return  number of children
   */
  override def numChildren(): Int = children.filter { _ != null }.length

  /**
   * Merge two nodes
   *
   * @param that the node which will be merged
   * @param trySplit flag indicating whether the node will be split
   * @return new node
   */
  override def merge(that: Node, trySplit: Boolean): Node = {
    if (!that.isInstanceOf[SplitNode]) this
    else {
      val splitNode = that.asInstanceOf[SplitNode]
      for (i <- 0 until children.length)
        this.children(i) = (this.children(i)).merge(splitNode.children(i), trySplit)
      this
    }
  }

  /**
   * Returns the node description
   * @return String containing the description
   */
  override def description(): String = {
    val sb = new StringBuffer("  " * nodeDepth + "\n")
    val testDes = conditionalTest.description()
    for (i <- 0 until children.length) {
      sb.append("  " * nodeDepth + " if " + testDes(i) + "\n")
      sb.append("  " * nodeDepth + children(i).description())
    }
    sb.toString()
  }

  override def toString(): String = "level[" + nodeDepth + "] SplitNode"

}
/**
 * Learning node class type for Hoeffding trees.
 */
abstract class LearningNode(classDistribution: Array[Double]) extends Node(classDistribution)
    with Serializable {

  /**
    * Learn and update the node
    *
    * @param ht a Hoeffding tree model
    * @param vector the input instance
    */
  def learn(ht: HoeffdingTreeModel, vector: Vector, classLabel: Int, weight: Double): Unit

  /**
   * Return whether a learning node is active
   */
  def isActive(): Boolean

  /**
    * Filter the data to the related leaf node
    *
    * @param vector the input instance
    * @param parent the parent of current node
    * @param index the index of current node in the parent children
    * @return FoundNode containing the leaf node
    */
  override def filterToLeaf(vector: Vector, parent: SplitNode, index: Int) = {
    new FoundNode(this, parent, index)
  }
}

/**
 * Basic majority class active learning node for Hoeffding tree
 */
class ActiveLearningNode(classDistribution: Array[Double])
    extends LearningNode(classDistribution) with Serializable {

  var addonWeight: Double = 0

  var blockAddonWeight: Double = 0

  var schema: StructType = null

  var featureObservers: Array[FeatureClassObserver] = null

  def this(classDistribution: Array[Double], schema: StructType) {
    this(classDistribution)
    this.schema = schema

    init()
  }

  def this(that: ActiveLearningNode) {
    this(Utils.addArrays(that.classDistribution, that.blockClassDistribution), that.schema)
    this.addonWeight = that.addonWeight
  }

  /**
   * init featureObservers array
   */
  def init(): Unit = {
    if (featureObservers == null) {
      // Use num_attrs from X, such that X was created from VectorAssembler
//      featureObservers = new Array(this.schema("X").metadata.getMetadataArray("ml_attr")("num_attrs").toInt)
      featureObservers = new Array(this.schema("X").metadata.getMetadata("ml_attr").getLong("num_attrs").toInt)
      for (i <- featureObservers.indices ) {
        featureObservers(i) = FeatureClassObserver.createFeatureClassObserver(classDistribution.length, i, this.schema)
      }
    }
  }

  /**
    * Learn and update the node
    *
    * @param ht a Hoeffding tree model
    * @param vector the input instance
    */
  override def learn(ht: HoeffdingTreeModel, vector: Vector, classLabel: Int, weight: Double): Unit = {
    init()
//    println("ActiveLearningNode.learn classLabel = " + classLabel + " featureObservers.length = " + featureObservers.length + " vector = ")
//    for(v <- vector.toArray) {
//      print(v + ",")
//    }

    blockClassDistribution(classLabel) += weight
    featureObservers.zipWithIndex.foreach {
      x => x._1.observeClass(classLabel, vector(x._2), weight)
    }
  }

  /**
   * Disable a feature at a given index
   *
   * @param fIndex the index of the feature
   */
  def disableFeature(fIndex: Int): Unit = {
    // TODO: not supported yet
  }

  /**
   * Returns whether a node is active.
   *
   */
  override def isActive(): Boolean = true

  /**
   * Returns whether a node is pure, which means it only has examples belonging
   * to a single class.
   */
  def isPure(): Boolean = {
    this.classDistribution.filter(_ > 0).length <= 1 &&
      this.blockClassDistribution.filter(_ > 0).length <= 1
  }

  def weight(): Double = { classDistribution.sum }

  def blockWeight(): Double = blockClassDistribution.sum

  def addOnWeight(): Double = {
    addonWeight
  }

  /**
   * Merge two nodes
   *
   * @param that the node which will be merged
   * @param trySplit flag indicating whether the node will be split
   * @return new node
   */
  override def merge(that: Node, trySplit: Boolean): Node = {
//    println("~ActiveLearningNode.merge with trySplit = " + trySplit)
    if (that.isInstanceOf[ActiveLearningNode]) {
      val node = that.asInstanceOf[ActiveLearningNode]
      //merge addonWeight and class distribution
      if (!trySplit) {
//        this.blockAddonWeight += that.blockClassDistribution.sum
//        for (i <- 0 until blockClassDistribution.length)
//          this.blockClassDistribution(i) += that.blockClassDistribution(i)
        // TODO: Review this code to verify if it is possible to get rid of the "trySplit" strategy
      } else {
        this.blockAddonWeight += that.blockClassDistribution.sum
        for (i <- 0 until blockClassDistribution.length)
          this.blockClassDistribution(i) += that.blockClassDistribution(i)

        this.addonWeight = node.blockAddonWeight

//        print("\t~Going to update the classDistribution, node.blockAddonWeight = " + node.blockAddonWeight + " that.blockClassDistribution = ")
//        for(block <- that.blockClassDistribution)
//          print(block + " ")
//        println()

        for (i <- 0 until classDistribution.length)
          this.classDistribution(i) += that.blockClassDistribution(i)
      }
      //merge feature class observers
      for (i <- 0 until featureObservers.length)
        featureObservers(i) = featureObservers(i).merge(node.featureObservers(i), trySplit)
    }
    this
  }
  /**
   * Returns Split suggestions for all features.
   *
   * @param splitCriterion the SplitCriterion used
   * @param ht a Hoeffding tree model
   * @return an array of FeatureSplit
   */
  def getBestSplitSuggestions(splitCriterion: SplitCriterion, ht: HoeffdingTreeModel): Array[FeatureSplit] = {
    val bestSplits = new ArrayBuffer[FeatureSplit]()
    featureObservers.zipWithIndex.foreach(x =>
      bestSplits.append(x._1.bestSplit(splitCriterion, classDistribution, x._2, ht.binaryOnly)))
    if (!ht.noPrePrune) {
      bestSplits.append(new FeatureSplit(null, splitCriterion.merit(classDistribution,
        Array.fill(1)(classDistribution)), new Array[Array[Double]](0)))
    }
    bestSplits.toArray
  }

  override def toString(): String = "level[" + nodeDepth + "]ActiveLearningNode:" + Utils.arraytoString(classDistribution)
}
/**
 * Inactive learning node for Hoeffding trees
 */
class InactiveLearningNode(classDistribution: Array[Double])
    extends LearningNode(classDistribution) with Serializable {

  def this(that: InactiveLearningNode) {
    this(Utils.addArrays(that.classDistribution, that.blockClassDistribution))
  }

//  /**
//    * Learn and update the node. No action is taken for InactiveLearningNode
//    *
//    * @param ht      HoeffdingTreeModel
//    * @param example an Example will be processed
//    */
  //  override def learn(ht: HoeffdingTreeModel, example: Example): Unit = {}

  override def learn(ht: HoeffdingTreeModel, vector: Vector, classLabel: Int, weight: Double): Unit = {}

  /**
   * Return whether a learning node is active
   */
  override def isActive(): Boolean = false

  /**
   * Merge two nodes
   *
   * @param that the node which will be merged
   * @param trySplit flag indicating whether the node will be split
   * @return new node
   */
  override def merge(that: Node, trySplit: Boolean): Node = this

  override def toString(): String = "level[" + nodeDepth + "] InactiveLearningNode"
}
