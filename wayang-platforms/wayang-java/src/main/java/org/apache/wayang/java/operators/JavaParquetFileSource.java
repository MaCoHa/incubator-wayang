package org.apache.wayang.java.operators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator {

    public JavaParquetFileSource(String inputUrl) {
        super(inputUrl);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);

    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OperatorContext operatorContext) {

     
        final Configuration config = new Configuration();
        Path path = new Path(this.getInputUrl());

        try {
            InputFile inputFile = HadoopInputFile.fromPath(path, config);

            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                Stream<GenericRecord> stream = Stream.generate(() -> {
                    try {
                        return reader.read();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).takeWhile(Objects::nonNull);

                ((org.apache.wayang.java.channels.StreamChannel.Instance) outputs[0]).accept(stream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        outputs[0].getLineage().addPredecessor(prepareLineageNode);
        return prepareLineageNode.collectAndMark();

    }

}
