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
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lambda Pass.
 * Description: A part of lambda executor, assigning LambdaResourceProperty
 */
@Annotates(CommunicationPatternProperty.class)
public final class CustomCommunicationPass extends AnnotatingPass {

  public CustomCommunicationPass() {
    super(CustomCommunicationPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    Pattern pattern = Pattern.compile("Communication=(O|S|B)");
    dag.getVertices().forEach(vertex -> {
      if (vertex instanceof SourceVertex) return;
      if (vertex instanceof OperatorVertex) {
        Matcher matcher = pattern.matcher(((OperatorVertex) vertex).getTransform().toString());
        if (matcher.find()) {
          CommunicationPatternProperty.Value communicationPattern = null;
          if (matcher.group(1).equals("O")) {
            communicationPattern = CommunicationPatternProperty.Value.ONE_TO_ONE;
          } else if (matcher.group(1).equals("S")) {
            communicationPattern = CommunicationPatternProperty.Value.SHUFFLE;
          } else if (matcher.group(1).equals("F")) {
            communicationPattern = CommunicationPatternProperty.Value.BROADCAST;
          }
          for (IREdge edge: dag.getOutgoingEdgesOf(vertex)){
            edge.setProperty(CommunicationPatternProperty.of(communicationPattern));
          }
        }
      }
    });

    return dag;
  }
}
