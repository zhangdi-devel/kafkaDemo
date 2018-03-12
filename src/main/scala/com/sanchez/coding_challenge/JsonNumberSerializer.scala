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


import java.util

import org.apache.kafka.common.serialization.Serializer
import spire.math.Number

/**
  * A very naive serializer
  * */
class JsonNumberSerializer extends Serializer[List[Number]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: List[Number]): Array[Byte] = {
    data match {
      case Nil => Array[Byte]()
      case head :: tail => s"""\"dummy\": $head""".toCharArray.map(_.toByte) ++ serialize(topic, tail)
    }
  }
}
