/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.sanchez.coding_challenge
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import scala.collection.JavaConverters.asJavaIterableConverter
import spire.math.Number
import com.datastax.driver.core._
import org.slf4j.{Logger, LoggerFactory}

object KafkaDemo {
  val logger: Logger =  LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val inputTopic = "JSON_Topic"

    /** config */
    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "extract-numbers-application7")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      /** this interval determines how fast to publish sums to downstream */
      p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, new Integer(100))
      p
    }

    /** using the stream api*/
    val builder: StreamsBuilder = new StreamsBuilder()

    /** map json token to List[Number] using the custom serde classes */
    val numberLstSerializer = new JsonNumberSerializer
    val numberLstDeserializer = new JsonNumberDeserializer
    val numberLstSerder: Serde[List[Number]] = Serdes.serdeFrom(numberLstSerializer, numberLstDeserializer)
    val numberSerder: Serde[Number] = Serdes.serdeFrom(new NumberSerializer, new NumberDeserializer)
    val numberLsts: KStream[String, List[Number]] =
      builder.stream(inputTopic, Consumed.`with`(Serdes.String(), numberLstSerder))

    /** only keep numbers */
    val numbers: KStream[String, Number] = numberLsts.flatMapValues{ numberLst =>
      numberLst.asJava
    }

    /** only keep positive numbers */
    val positiveNumbers: KStream[String, Number] = numbers.filter(IsPositive)

    /** aggregate */
    val keyedNumbers: KStream[String, Number] = numbers.selectKey((_, _) => "1")
    val strNum: Serialized[String, Number] = Serialized.`with`(Serdes.String(), numberSerder)
    val sums: KTable[String, Number] = keyedNumbers.groupByKey(strNum).reduce(AddNumber)

    /** publish to new topics */

    sums.toStream().to("Topic_A", Produced.`with`(Serdes.String(), numberSerder))
    positiveNumbers.to("Topic_B", Produced.`with`(Serdes.String(), numberSerder))

    /** write sums to cassandra */

    val cluster = setupCassandra()
    val session = cluster.connect("KafkaDemo")
    val action = new UpdateCassandra(session)
    sums.toStream().foreach(action)


    /** print topology and run */
    val topology = builder.build()
    logger.info(topology.describe().toString)
    val streams: KafkaStreams = new KafkaStreams(topology, config)
    logger.info("start streams")
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
      //cluster.close()
    }))
  }


  def setupCassandra(): Cluster = {
    val cn = "Test Cluster"
    val cp = "127.0.0.1"
    val cluster = Cluster.builder().withClusterName(cn).addContactPoint(cp).build()
    val session = cluster.connect()
    session.execute("drop keyspace if exists KafkaDemo")
    session.execute("create keyspace KafkaDemo with replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}")
    session.execute("use KafkaDemo")
    session.execute("create table sums ( tid TIMEUUID primary key, num double)")
    //cluster.close()
    cluster
  }

}

