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

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.util

import org.apache.kafka.common.serialization.Deserializer
import org.json4s.native.JsonParser._
import spire.math.Number

/**
  * This class Deserialize Json Tokens from Stream
  * Only keep numeric values
  * */

class JsonNumberDeserializer extends Deserializer[List[Number]] {

  /** use this custom parser to parse the JSON stream */
  private val parser: Parser => List[Number] = {p =>
    def helper(psr: Parser, acc: List[Number]): List[Number] = psr.nextToken match {
      case End => acc
      case IntVal(x) => helper(psr, Number(x) :: acc)
      case LongVal(x) => helper(psr, Number(x) :: acc)
      case DoubleVal(x) => helper(psr, Number(x) :: acc)
      case BigDecimalVal(x) => helper(psr, Number(x) :: acc)
      case _ => helper(psr, acc)
    }
    helper(p, List[Number]()).reverse
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): List[Number] = {
    val stream = new ByteArrayInputStream(data)
    val reader = new InputStreamReader(stream)
    parse(reader, parser)
  }

  override def close(): Unit = {}
}
