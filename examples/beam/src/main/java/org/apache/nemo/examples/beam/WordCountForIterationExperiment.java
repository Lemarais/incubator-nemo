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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * WordCount application.
 */
public final class WordCountForIterationExperiment {

  static class DoNothingFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element final String words, final OutputReceiver<String> receiver) {
      receiver.output(words);
    }
  }

  static class IterationFn extends DoFn<String, String> {
    private int iteration;

    public IterationFn(int iteration) {
      this.iteration = iteration;
    }

    @ProcessElement
    public void processElement(@Element String words, final OutputReceiver<String> receiver) {
      for (int i = 0 ; i < this.iteration; i++) {
        words = words.replace(" +", "#");
        words = words.replace("#+", " ");
      }
      receiver.output(words);
    }
  }

  /**
   * Private Constructor.
   */
  private WordCountForIterationExperiment() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final List<Integer> iterations = Arrays.stream(args[1].split(" ")).map(Integer::valueOf).collect(Collectors.toList());
    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WordCountForExperiment");

    final Pipeline p = generateWordCountPipeline(options, inputFilePath, iterations);
    p.run().waitUntilFinish();
  }

  /**
   * Static method to generate the word count Beam pipeline.
   *
   * @param options        options for the pipeline.
   * @param inputFilePath  the input file path.
   * @param iterations     the output file path.
   * @return the generated pipeline.
   */
  static Pipeline generateWordCountPipeline(final PipelineOptions options,
                                            final String inputFilePath, final List<Integer> iterations) {

    final Pipeline p = Pipeline.create(options);

    final List<PCollection<String>> data = new ArrayList<>();
    data.add(GenericSourceSink.read(p, inputFilePath));
    for (int i = 0; i < iterations.size() ; i++) {
      int iteration = iterations.get(i);
      final PCollection<String> newData = data.get(i)
        .apply(String.format("Group=1 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=1 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=2 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=3 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=3 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=3 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=4 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=5 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=5 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=5 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=5 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=6 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=7 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=7 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=7 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=8 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=9 Iteration=%d Store=F", iteration), ParDo.of(new IterationFn(iteration)))
        .apply(String.format("Group=9 Iteration=%d Store=M", iteration), ParDo.of(new IterationFn(iteration)));

      data.add(newData);
    }

    return p;
  }
}
