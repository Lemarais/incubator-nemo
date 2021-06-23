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
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ExecutorSelectionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pass for initiating IREdge data persistence ExecutionProperty with default values.
 */
@Annotates(ExecutorSelectionProperty.class)
public final class CustomExecutorSelectPass extends AnnotatingPass {
  private static final Logger LOG = LoggerFactory.getLogger(CustomExecutorSelectPass.class);
  private static int executorId;
  /**
   * Default constructor.
   */
  public CustomExecutorSelectPass() {
    super(CustomExecutorSelectPass.class);
    executorId = 0;
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.getVertices().forEach(vertex -> {
      vertex.setProperty(ExecutorSelectionProperty.of(executorId));
      executorId = (executorId + 1) % 2;
    });
    return dag;
  }
}
