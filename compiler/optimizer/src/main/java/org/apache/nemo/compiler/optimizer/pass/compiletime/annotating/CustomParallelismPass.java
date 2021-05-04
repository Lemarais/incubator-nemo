/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

import javax.xml.transform.Source;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lambda Pass.
 * Description: A part of lambda executor, assigning LambdaResourceProperty
 */
@Annotates(ParallelismProperty.class)
public final class CustomParallelismPass extends AnnotatingPass {

  public CustomParallelismPass() {
    super(CustomParallelismPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    Pattern pattern = Pattern.compile("P=([0-9]+)");

    dag.getVertices().forEach(vertex -> {
//      if (vertex instanceof SourceVertex) return;
//      if (vertex instanceof OperatorVertex) {
//        Matcher matcher = pattern.matcher(((OperatorVertex) vertex).getTransform().toString());
//        if (matcher.find()) {
//          int parallelism = Integer.parseInt(matcher.group(1));
//          vertex.setProperty(ParallelismProperty.of(parallelism));
      vertex.setProperty(ParallelismProperty.of(3));

//        }
//      }
    });

    return dag;
  }
}
