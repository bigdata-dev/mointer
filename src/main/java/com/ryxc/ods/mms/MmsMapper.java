package com.ryxc.ods.mms;


import com.ryxc.utils.Metadata;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2015/12/27.
 */
public class MmsMapper extends Mapper<Object, Text, Text, MmsRecord> {
    private static Pattern pattern = Pattern.compile("\\,");
    SimpleDateFormat sdf =  new SimpleDateFormat("yyyMMddHH");

    public static Text keytext = new Text();
    public  static MmsRecord mr = new MmsRecord();

    /*字段位置*/
    static int INTERFACE = 0 ;
    static int IMSI = 0;
    static int IMEI = 0;
    static int MSISDN = 0;
    static int USER_IP = 0;
    static int FLOW = 0;

    /*初始化参数*/
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Metadata rc = new Metadata("/config/MmsDemo.properties");
        INTERFACE = Integer.parseInt(rc.getValue("INTERFACE"));//内容是0 字段位置
        IMSI = Integer.parseInt(rc.getValue("IMSI"));//内容是1 字段位置
        IMEI = Integer.parseInt(rc.getValue("IMEI"));//内容是2 字段位置
        MSISDN = Integer.parseInt(rc.getValue("MSISDN"));//内容是3 字段位置
        USER_IP = Integer.parseInt(rc.getValue("USER_IP"));//内容是4 字段位置
        FLOW = Integer.parseInt(rc.getValue("FLOW"));//内容是5 字段位置

    }

    /*业务逻辑*/
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] gn_data = pattern.split(value.toString(), -1);

        if(gn_data.length == 6){//过滤掉垃圾数据

            String starttime = sdf.format(new Date());

            StringBuffer sb_key = new StringBuffer();
            sb_key.append(starttime).append("|");
            sb_key.append(StringUtils.isBlank(gn_data[INTERFACE])?"99999":gn_data[INTERFACE]).append("|"); //清洗
            sb_key.append(StringUtils.isBlank(gn_data[IMSI])?"99999":gn_data[IMSI]).append("|"); //清洗
            sb_key.append(StringUtils.isBlank(gn_data[IMEI])?"99999":gn_data[IMEI]).append("|"); //清洗
            sb_key.append(StringUtils.isBlank(gn_data[MSISDN])?"99999":gn_data[MSISDN]).append("|"); //清洗
            sb_key.append(StringUtils.isBlank(gn_data[USER_IP])?"99999":gn_data[USER_IP]); //清洗
            keytext.set(sb_key.toString());

            mr.flow = gn_data[FLOW];

            context.write(keytext,mr);
        }else{
            System.out.println("gn_data.length == " + gn_data.length);
        }
    }

    /*关闭资源*/
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
