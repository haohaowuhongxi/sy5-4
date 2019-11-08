package sy544;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<LongWritable, Text, Text, Data> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context)
            throws IOException, InterruptedException{
        //数据：00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html
        String data = v1.toString();
        //分词
        String[] words = data.split("\t");
        //创建对象
        Data d = new Data();
        //设置属性
        //时间
        d.setTime(words[0]);
        //数字编号
        d.setNumber(words[1]);
        //名称
        d.setName(words[2]);
        //再分组
        String[] words3 = words[3].split(" ");
        //排名
        d.setRank(Integer.parseInt(words3[0]));
        //顺序
        d.setOrder(Integer.parseInt(words3[1]));
        //网站地址
        d.setWed(words[4]);
        //输出
        context.write(new Text(d.getName()), d);
    }
}
