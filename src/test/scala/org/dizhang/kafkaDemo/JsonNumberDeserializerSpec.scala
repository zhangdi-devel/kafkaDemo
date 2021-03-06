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

package org.dizhang.kafkaDemo

import spire.math.Number
class JsonNumberDeserializerSpec extends UnitSpec {
  private val des = new JsonNumberDeserializer

  implicit class SerializedJson(json: String) {
    def toByteArray: Array[Byte] = {
      json.toCharArray.map(_.toByte)
    }
  }

  "A JsonNumberDeserializer" should "deserialize JSON" in {
    val test = """{"a": 1, "b": 2.0, "c"}"""

    logger.info("test:" + des.deserialize("", test.toByteArray).mkString(","))
  }
}
