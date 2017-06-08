/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, AllTuples, UnspecifiedDistribution}
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap

case class SpillableAggregate(
                               partial: Boolean,
                               groupingExpressions: Seq[Expression],
                               aggregateExpressions: Seq[NamedExpression],
                               child: SparkPlan) extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
    * An aggregate that needs to be computed for each row in a group.
    *
    * @param unbound Unbound version of this aggregate, used for result substitution.
    * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
    * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
    *                        output.
    */
  case class ComputedAggregate(
                                unbound: AggregateExpression,
                                aggregate: AggregateExpression,
                                resultAttribute: AttributeReference)

  /** Physical aggregator generated from a logical expression.  */
  private[this] val aggregator: ComputedAggregate = {
    /* [Yamato's Note] - Translation
    Create a new seq[ComputedAggregate]
    for each namedExpression in the aggregateExpressions:
      for each expression in the namedExpression: (namedExpression = seq[Expression])
        Check if the expression is an AggregateExpression. If it is, create ComputeAggregate corresponding to that AggregateExpression.
        Add this to seq created in the first line.

    Take the first element of seq[ComputedAggregate] */
    aggregateExpressions.flatMap { agg =>
      agg.collect {
        case a: AggregateExpression =>
          ComputedAggregate(
            a,
            BindReferences.bindReference(a, child.output),
            AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
      }
    }.head
  } // IMPLEMENTED

  /** Schema of the aggregate.  */
    /* [Yamato's Note]Consider the fact that val aggregator is simply ONE of the elements of computedAggregates in Aggregate.scala.
    This is equivalent to val computeSchema of Aggregate.Scala.
     */
  private[this] val aggregatorSchema: AttributeReference = aggregator.resultAttribute // IMPLEMENTED

  /** Creates a new aggregator instance.  */
  /* [Yamato's Note]same logic. Equivalent of newAggregateBuffer */
  private[this] def newAggregatorInstance(): AggregateFunction = aggregator.aggregate.newInstance() // IMPLEMENTED

  /** Named attributes used to substitute grouping attributes in the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
    * A map of substitutions that are used to insert the aggregate expressions and grouping
    * expression into the final result expression.
    */
  protected val resultMap =
  ( Seq(aggregator.unbound -> aggregator.resultAttribute) ++ namedGroups).toMap

  /**
    * Substituted version of aggregateExpressions expressions which are used to compute final
    * output rows given a group and the result of all aggregate computations.
    */
  private[this] val resultExpression = aggregateExpressions.map(agg => agg.transform {
    case e: Expression if resultMap.contains(e) => resultMap(e)
  }
  )

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions(iter => generateIterator(iter))
  }

  /**
    * This method takes an iterator as an input. The iterator is drained either by aggregating
    * values or by spilling to disk. Spilled partitions are successively aggregated one by one
    * until no data is left.
    *
    * @param input the input iterator
    * @param memorySize the memory size limit for this aggregate
    * @return the result of applying the projection
    */
  /* [Yamato's Note] when looking at this generateIterator Method, look in the order of:
  1. The first three value declarations
  2. aggregate() Method
  3. CS143Utils.AggregateIteratorGenerator object
  4. Iterator[Row] part of generateIterator

  This order roughly corresponds to that of Aggregate.scala, lines 148-191.
  */
  def generateIterator(input: Iterator[Row], memorySize: Long = 64 * 1024 * 1024, numPartitions: Int = 64): Iterator[Row] = {
    /* [Yamato's Note]these things do exactly the same thing as line 149~150 of Aggregate.scala. SizeTrackingAppendOnlyMap also tracks size of the map (used later in part 6 or 7, probably)
    */
    val groupingProjection = CS143Utils.getNewProjection(groupingExpressions, child.output) // val groupingProjection in Aggregate.scala
    var currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction] // val hashTable in Aggregate.scala
    var data = input



    def initSpills(): Array[DiskPartition]  = {
      /* IMPLEMENT THIS METHOD */
      val dpArr = new Array[DiskPartition](numPartitions)
      for(i <- 0 to numPartitions - 1){
        dpArr(i) = new DiskPartition("spill" + i, 0)
      }

      dpArr

    }

    val spills = initSpills()

    new Iterator[Row] {
      var aggregateResult: Iterator[Row] = aggregate()

      /* [Yamato's Note] In the SpillableAggregate.scala, AggregateIteratorGenerator does all the hard work (which corresponds to lines 169-191 in Aggregate.scala).
      Therefore, the hasNext() and next() simply iterates over iterators produced by Generator.
       */
      def hasNext() = {
        /* IMPLEMENT THIS METHOD */
          aggregateResult.hasNext || fetchSpill()
      }

      def next() = {
        /* IMPLEMENT THIS METHOD */
        if(!aggregateResult.hasNext && !fetchSpill()){
          throw new NoSuchElementException("no data")
        }
        else{
          if(!aggregateResult.hasNext){
            fetchSpill()
          }
          aggregateResult.next()
        }
      }

      /**
        * This method load the aggregation hash table by draining the data iterator
        *
        * @return
        */
      private def aggregate(): Iterator[Row] = {
        /* IMPLEMENT THIS METHOD */
        // [Yamato's Note] this basically does the same thing as 153~160 of Aggregate.scala.
        // get method is replaced with apply method because class AppendOnlyMap overrides apply method with what get method for normal hashMap does
        // newAggregatorInstance is the buffer for this single aggregate expression
        // update method is used instead of put method (because again, it does the same thing)

        var currentRow: Row = null
        var diskHashedRelation: DiskHashedRelation = null
        while (data.hasNext) {
          currentRow = data.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = currentAggregationTable(currentGroup)
          // if the group of new row being added does not already exist in the hash table
          if (currentBuffer == null) {
            currentBuffer = newAggregatorInstance()
            // if has potential to spill
            if (data == input && CS143Utils.maybeSpill(currentAggregationTable, memorySize)) {
              // spill
              spillRecord(currentRow)
            }
            // no potential to spill, create new entry and add
            else {
              currentAggregationTable.update(currentGroup.copy(), currentBuffer)
              currentBuffer.update(currentRow)
            }
          }
            // if record already exists, no spilling. just add the record
          else{
            currentBuffer.update(currentRow)
          }

        }

        if(data == input){
          for(i <-0 to numPartitions - 1){
            spills(i).closeInput()
          }
          diskHashedRelation = new GeneralDiskHashedRelation(spills)
          hashedSpills = Some(diskHashedRelation.getIterator())
        }

        val generateAggIt = AggregateIteratorGenerator(resultExpression, Seq(aggregatorSchema) ++ namedGroups.map(_._2)) // look at lines 174 of Aggregate.scala
        generateAggIt(currentAggregationTable.iterator) // pass in key-value pair of current hashMap to make it do the work over each group - aggregate pair. Returns Iterator[Row] of processed rows
      }

      /**
        * Spill input rows to the proper partition using hashing
        *
        * @return
        */
      private def spillRecord(row: Row)  = {
        /* IMPLEMENT THIS METHOD */
        val inputKey = groupingProjection(row).hashCode() % numPartitions
        spills(inputKey).insert(row)
      }

      /**
        * Global object wrapping the spills into a DiskHashedRelation. This variable is
        * set only when we are sure that the input iterator has been completely drained.
        *
        * @return
        */
      var hashedSpills: Option[Iterator[DiskPartition]] = None

      /**
        * This method fetches the next records to aggregate from spilled partitions or returns false if we
        * have no spills left.
        *
        * @return
        */
      private def fetchSpill(): Boolean  = {
        /* IMPLEMENT THIS METHOD */
        if(aggregateResult.hasNext){
          false
        }
        else if(hashedSpills.isEmpty){
          false
        }
        else{
          val dpIt = hashedSpills.get // iterator of hashedSpills

          var hasPartition = false
          while(dpIt.hasNext && !hasPartition){
            data = dpIt.next().getData()
            if(data.hasNext){
              hasPartition = true
            }
          }

          if(!hasPartition){
            false
          }

          else{
            currentAggregationTable = new SizeTrackingAppendOnlyMap[Row, AggregateFunction]
            aggregateResult = aggregate()
            true
          }

        }
      }
    }
  }
}
