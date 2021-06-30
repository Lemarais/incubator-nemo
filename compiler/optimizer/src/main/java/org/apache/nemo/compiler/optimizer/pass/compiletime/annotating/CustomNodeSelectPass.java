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
import org.apache.nemo.common.ir.vertex.executionproperty.NodeSelectionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pass for initiating IREdge data persistence ExecutionProperty with default values.
 */
@Annotates(NodeSelectionProperty.class)
public final class CustomNodeSelectPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(CustomNodeSelectPass.class.getName());
  static String SourceNode = null;
  /**
   * Default constructor.
   */
  public CustomNodeSelectPass(String sourceNode) {
    super(CustomNodeSelectPass.class);
    SourceNode = sourceNode;
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    Pattern pattern = Pattern.compile("Node=([^\\s]+)");
    dag.getVertices().forEach(vertex -> {
      if (vertex instanceof SourceVertex) {
        LOG.error(SourceNode);
        vertex.setProperty(NodeSelectionProperty.of(SourceNode));
      }
      if (vertex instanceof OperatorVertex) {
        Matcher matcher = pattern.matcher(((OperatorVertex) vertex).getTransform().toString());
        if (matcher.find()) {
          String NodeName = matcher.group(1);
          LOG.error(NodeName);
          vertex.setProperty(NodeSelectionProperty.of(NodeName));
        }
      }
    });
    return dag;
  }
}
