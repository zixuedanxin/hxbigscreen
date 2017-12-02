package com.dxhy.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;

import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by drguo on 2017/12/1.
 */
public class Hdfs2Solr {

    private static final Logger LOGGER = getLogger(Hdfs2Solr.class);

    public static class Hdfs2SolrMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {

            context.write(key, value);
        }

    }

    public static class Hdfs2SolrReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                context.write(v, NullWritable.get());
            }
        }
    }

    public static class Hdfs2SolrRunner extends Configured implements Tool {
        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration();
            //集群调优
            conf.set("mapreduce.map.output.compress", "true");
            conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            conf.set("mapreduce.map.java.opts", "-Xmx9216m");
            conf.set("mapreduce.reduce.java.opts", "-Xmx9216m");
            conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.08");
            conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");
            //设置map任务切片大小
            conf.set("mapreduce.input.fileinputformat.split.minsize", "1073741824");
            conf.set("mapreduce.input.fileinputformat.split.minsize", "1073741824");
            //设置排序缓冲区
            conf.set("mapreduce.task.io.sort.mb", "1200");
            //设置任务超时时间
            conf.set("mapreduce.task.timeout", "0");
            //设置jvm重用
            conf.set("mapreduce.job.jvm.numtasks", "-1");
            //跳过坏记录
            conf.set("mapreduce.reduce.skip.maxgroups", "Long.MAX_VALUE");
            conf.set("mapreduce.job.skip.outdir", "/data1/zdh/yarn/local/badrecords");
            conf.set("mapreduce.task.skip.start.attempts", "1");
            conf.set("mapreduce.reduce.maxattempts", "10");
            //关闭推测执行
            conf.set("mapreduce.map.speculative", "false");
            conf.set("mapreduce.reduce.speculative", "false");
            Job job = Job.getInstance(conf, "Hdfs2Solr");
            job.setJarByClass(Hdfs2Solr.Hdfs2SolrRunner.class);

            job.setMapperClass(Hdfs2Solr.Hdfs2SolrMapper.class);
            job.setReducerClass(Hdfs2Solr.Hdfs2SolrReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, args[0]);
            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path(args[1]);
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);//true递归删除
            }
            //指定处理结果所存放的路径
            FileOutputFormat.setOutputPath(job, path);
            //显示进度
            boolean b = job.waitForCompletion(true);
            LOGGER.info("是否成功运行：" + b);
            return b ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new Hdfs2Solr.Hdfs2SolrRunner(), args);
        System.exit(res);
    }
}
