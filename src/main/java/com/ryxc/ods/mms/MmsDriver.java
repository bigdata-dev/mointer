package com.ryxc.ods.mms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2015/12/27.
 */
public class MmsDriver extends Configured implements Tool {

    //设置hadoop参数
    @Override
    public int run(String[] args) throws Exception {
        //参数校验
        if (args.length != 2) {
            System.out.println(" Usage : <inputpath> <outpathpath>");
            ToolRunner.printGenericCommandUsage(System.out);//输出执行过程中打印的日志
            return -1;
        }

        //获取配置
        Configuration conf = getConf();

        //设置job基本参数
        Job job = new Job(conf);
        job.setJarByClass(MmsDriver.class);
        job.setJobName("Mms Driver");
        job.setMapperClass(MmsMapper.class);
        job.setReducerClass(MmsReducer.class);

        //设置输入输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MmsRecord.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //生成sequencefile
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        SequenceFileOutputFormat.setCompressOutput(job,true);
//        SequenceFileOutputFormat.setOutputCompressorClass(job,SnappyCodec.class);
//        SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK);//块压缩

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new MmsDriver(), args);//使用ToolRunner启动MR
        System.exit(run);
    }
}
