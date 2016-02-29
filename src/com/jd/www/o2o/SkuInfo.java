package com.jd.www.o2o;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class SkuInfo {
	String dealDateStr;
	public void run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "skuInfo");
		dealDateStr = conf.get("dealDate");
		
		job.setJarByClass(com.jd.www.o2o.SkuInfo.class);
		job.setMapperClass(Skuo2oMapper.class);
		job.setMapperClass(StockMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(skuInfoReducer.class);
		// TODO: specify output types
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		MultipleInputs.addInputPath(job, new Path("hdfs://ns1/user/dd_edw/gdm.db/gdm_m03_item_sku_o2o_da/dt="+dealDateStr), LzoTextInputFormat.class, Skuo2oMapper.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://ns1/user/dd_edw/fdm.db/fdm_stock_center_stock_center_chain/dp=ACTIVE/dt=4712-12-31/end_date=4712-12-31/"), LzoTextInputFormat.class, StockMapper.class);
		
		Path out = new Path("/user/mart_o2o/tmp.db/skuInfo");
		FileSystem fileSystem = FileSystem.get(conf); 
		if (fileSystem.exists(out)) {
			fileSystem.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job, out);

		if (!job.waitForCompletion(true))
			return;		
	}
	public static void main(String[] args) throws Exception {
		new SkuInfo().run(args);
	}
	
	public static class Skuo2oMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] columns = value.toString().split("\t");
			String skuId = columns[0];
			String skuName = columns[1].replaceAll("[,\\n\\t\\r]", "");
			String classId = columns[30];
			String className = columns[31];
			if (skuName.indexOf("测试") == -1 && classId.matches("[0-9]+")) {
				context.write(new Text(skuId), new Text("a"+"\t"+skuName+"\t"+classId+"\t"+className));
			}
		}
	}

	public static class StockMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] columns = value.toString().split("\t");
			String skuId = columns[3];
			String stationNo = columns[4];
			if (null != stationNo || !("".equals(stationNo))) {
				context.write(new Text(skuId), new Text("b"));
			}
		}
	}

	public static class skuInfoReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			boolean flag = false;
			Set<String> skuInfo = new HashSet<String>();
			for (Text val : values) {
				String[] columns = val.toString().split("\t");
				if ("b".equals(columns[0])) {
					flag = true;
				}else if ("a".equals(columns[0])) {
					skuInfo.add(columns[1]+"\t"+columns[2]+"\t"+columns[3]);
				}
			}
			if (flag) {
				Iterator<String> it = skuInfo.iterator();
				while(it.hasNext()) {
					context.write(key, new Text(it.next()));
				}
			}
		}
	}
}
