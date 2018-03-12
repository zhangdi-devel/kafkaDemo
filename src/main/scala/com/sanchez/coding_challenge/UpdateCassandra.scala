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

import com.datastax.driver.core.Session
import org.apache.kafka.streams.kstream.ForeachAction
import spire.math.Number

class UpdateCassandra(val session: Session) extends ForeachAction[String, Number] {

  override def apply(key: String, value: Number): Unit = {
    session.execute(s"insert into sums ( tid, num ) values ( now() , ${value.toDouble} ) ")
  }

}
