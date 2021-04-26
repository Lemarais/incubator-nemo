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
package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestArgs;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.compiler.optimizer.policy.SamplingLargeShuffleSkewPolicy;
import org.apache.nemo.examples.beam.policy.DataSkewPolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test PerKeyMedian program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class PerKeyMedianITCase {
  private static ArgBuilder builder;

  private static final String inputFileName = "inputs/test_input_median";
  private static final String outputFileName = "test_output_median";
  private static final String expectedOutputFileName = "outputs/expected_output_median";
  private static final String executorResourceFileName = ExampleTestArgs.getFileBasePath() + "executors/beam_test_executor_resources.json";
  private static final String inputFilePath = ExampleTestArgs.getFileBasePath() + inputFileName;
  private static final String outputFilePath = ExampleTestArgs.getFileBasePath() + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
      .addResourceJson(executorResourceFileName)
      .addUserMain(PerKeyMedian.class.getCanonicalName())
      .addUserArgs(inputFilePath, outputFilePath)
      .addDAGDirectory("/home/ubuntu/dag");
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureOutputValidity(ExampleTestArgs.getFileBasePath(), outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(ExampleTestArgs.getFileBasePath(), outputFileName);
    }
  }

  /**
   * Testing data skew dynamic optimization.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testDataSkew() throws Exception {
    JobLauncher.main(builder
      .addJobId(PerKeyMedianITCase.class.getSimpleName())
      .addOptimizationPolicy(DataSkewPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  /**
   * Testing large shuffle and data skew dynamic optimization.
   *
   * @throws Exception exception on the way.
   */
  @Test(timeout = ExampleTestArgs.TIMEOUT)
  public void testLargeShuffleSamplingSkew() throws Exception {
    JobLauncher.main(builder
      .addJobId(PerKeyMedianITCase.class.getSimpleName() + "_LargeShuffleSamplingSkew")
      .addOptimizationPolicy(SamplingLargeShuffleSkewPolicy.class.getCanonicalName())
      .build());
  }
}
