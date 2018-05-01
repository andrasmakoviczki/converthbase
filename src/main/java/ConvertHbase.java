import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by AMakoviczki on 2018. 04. 30..
 */
public class ConvertHbase {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertHbase.class);

    public static final String NAME = "ImportFromFile";
    public enum Counters { LINES }

    static class ImportMapper
            extends TableMapper<ImmutableBytesWritable, Put>{
    //extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
        private byte[] family = null;
        private byte[] qualifier = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String column = context.getConfiguration().get("conf.column");
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colkey[0];
            if (colkey.length > 1) {
                qualifier = colkey[1];
            }
        }

        public void map(LongWritable offset, Text line, Context context)
                throws IOException {
            try {
                String lineString = line.toString();
                byte[] rowkey = DigestUtils.md5(lineString);
                Put put = new Put(rowkey);
                put.addColumn(family, qualifier, Bytes.toBytes(lineString));
                context.write(new ImmutableBytesWritable(rowkey), put);
                context.getCounter(Counters.LINES).increment(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args)throws IOException, ClassNotFoundException, InterruptedException {
        /*if (args.length < 1) {
            System.out.println("Usage: convertsequence "
                    + " <hdfs_path>");
            System.exit(1);
        }

        String hdfsPath = args[0];
        String seqPath = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.app.mapreduce.am.staging-dir", "/tmp");

        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        Path inputPath = new Path(hdfsPath);
        Path outputPath = new Path(seqPath);

        if (!fs.exists(inputPath)) {
            System.out.println("Path " + inputPath + " does not exists");
            System.exit(1);
        }*/

        Configuration hconf = HBaseConfiguration.create();

        String input = args[0];
        String table = args[1];
        String column = args[2];

        //TODO: column
        hconf.set("conf.column", args[2]);
        Job job = new Job(hconf, "Import from file " + args[0] + " into table " + args[1]);

        job.setJarByClass(ConvertHbase.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
