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
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * WordCount application.
 */
public final class WordCountForExperiment {

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
  private WordCountForExperiment() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WordCountForExperiment");

    final Pipeline p = generateWordCountPipeline(options, inputFilePath, outputFilePath);
    p.run().waitUntilFinish();
  }

  /**
   * Static method to generate the word count Beam pipeline.
   *
   * @param options        options for the pipeline.
   * @param inputFilePath  the input file path.
   * @param outputFilePath the output file path.
   * @return the generated pipeline.
   */
  static Pipeline generateWordCountPipeline(final PipelineOptions options,
                                            final String inputFilePath, final String outputFilePath) {
    final Pipeline p = Pipeline.create(options);

    final PCollection<String> data = GenericSourceSink.read(p, inputFilePath);

    final PCollection<String> edgeTest = data
      .apply("Group=1 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=1 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=2 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=3 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=3 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=3 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=4 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=5 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=5 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=5 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=5 Store=M", ParDo.of(new DoNothingFn()))
      .apply("Group=6 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=7 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=7 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=7 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=8 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=9 Store=F", ParDo.of(new DoNothingFn()))
      .apply("Group=9 Store=M", ParDo.of(new DoNothingFn()));

    final PCollection<String> vertexTest = edgeTest
      .apply("Group=10", ParDo.of(new IterationFn(1000)))
      .apply("Group=11", ParDo.of(new IterationFn(2000)))
      .apply("Group=12", ParDo.of(new IterationFn(4000)));


    for (int i = 0; i < 2; i++) {
      vertexTest.apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
        @Override
        public KV<String, Long> apply(final String line) {
          final String[] words = line.split(" +");
          final String documentId = words[0] + "#" + words[1];
          final Long count = Long.parseLong(words[2]);
          return KV.of(documentId, count);
        }
      }))
      .apply(Sum.longsPerKey());
    }
    return p;
  }
}
