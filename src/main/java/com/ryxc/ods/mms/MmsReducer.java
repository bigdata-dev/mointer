package com.ryxc.ods.mms;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by Administrator on 2015/12/27.
 */
public class MmsReducer extends Reducer<Text,MmsRecord,NullWritable,Text>{

    public static Text keytext  = new Text();

    public static Text valuetext  = new Text();

    SimpleDateFormat sdf =  new SimpleDateFormat("yyyMMddHH");

    //定义MmsRecord实例 用于接收values中遍历出每一个对象
    public  static MmsRecord mr = new MmsRecord();

    //多目录输出
    private MultipleOutputs<NullWritable,Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //需要初始化
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

    }

    @Override
    protected void reduce(Text key, Iterable<MmsRecord> values, Context context) throws IOException, InterruptedException {
        SimpleDateFormat sdf =  new SimpleDateFormat("yyyMMddHH");

        Date date = new Date();
        double flow = 0;

        //获取values的迭代器
        Iterator<MmsRecord> ite = values.iterator();

        //遍历每一个MmsRecord，将指标字段遍历相加
        while(ite.hasNext()){
            mr = ite.next();
            flow = flow + Double.parseDouble(mr.flow);
        }
        flow = flow /(1000 * 1000);//按照兆算 原始数据是Byte

        //拼接输出内容 一般是value
        StringBuffer sb_value = new StringBuffer();
        sb_value.append(key).append("|");
        sb_value.append(String.format("%.2f", flow));//保留两位小数点
        valuetext.set(sb_value.toString());

        StringBuffer path = new StringBuffer();
        path.append("/user/ryxc/MmsTest/");
        path.append(sdf.format(date));
        path.append("-");

        System.out.println(
                "--------------valuetext = " + valuetext
        );

        //多目录输出
        multipleOutputs.write(NullWritable.get(),valuetext,path.toString());

    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        multipleOutputs.close();
    }
}
