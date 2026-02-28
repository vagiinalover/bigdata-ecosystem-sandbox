package com.example.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class SizeBasedParquetOutputFormat<T> extends ParquetOutputFormat<T> {

    @Override
    public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        final RecordWriter<Void, T> parentWriter = super.getRecordWriter(context);
        Configuration conf = context.getConfiguration();
        final long targetBytes = conf.getLong("spark.custom.targetSize", 128 * 1024 * 1024);

        return new RecordWriter<Void, T>() {
            private long rowsWritten = 0;
            // Estimating ~35 bytes per row based on previous sampling
            private final int bytesPerRowEstimate = 35;

            @Override
            public void write(Void key, T value) throws IOException, InterruptedException {
                parentWriter.write(key, value);
                rowsWritten++;
                
                if ((rowsWritten * bytesPerRowEstimate) > targetBytes) {
                   // Log warning
                }
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                parentWriter.close(context);
            }
        };
    }
}
