/**    
* @Title: CalTFMR.java  
* @Package www.jd.com.o2o  
* @Description: TODO(用一句话描述该文件做什么)  
* @author qiuxiangu@gmail.com    
* @date 2016年2月27日 下午7:54:22  
* @version V1.0    
*/

package com.jd.www.o2o;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jd.www.o2o.util.Bigram;

/**
 *  
 * 
 * @ClassName: CalTFMR 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author qiuxiangu@jd.com
 * @date 2016年2月27日 下午7:54:22     
 */

public class CalTFMR {
	public static class CalTFMapper extends Mapper<Object, Text, Text, Text> {
		Bigram br = null;
		/*
		 * (非 Javadoc)  <p>Title: setup</p>  <p>Description: </p> 
		 * 
		 * @param context
		 * 
		 * @throws IOException
		 * 
		 * @throws InterruptedException 
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
		 * Mapper.Context) 
		 */

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			br = new Bigram(2);
		}
		/*
		 * (非 Javadoc)  <p>Title: map</p>  <p>Description: </p> 
		 * 
		 * @param key
		 * 
		 * @param value
		 * 
		 * @param context
		 * 
		 * @throws IOException
		 * 
		 * @throws InterruptedException 
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context) 
		 */

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] columns = value.toString().split("\t");

			String skuName = columns[1].replace("\t", " ");
			String classId = columns[2];
			List<String> ram = br.splits(skuName);
			for (int i = 0; i < ram.size(); i++) {
				context.write(new Text(classId), new Text(ram.get(i)));
			}
		}

	}

	public static class CalTFReducer extends Reducer<Text, Text, Text, Text> {

		/*
		 * (非 Javadoc)  <p>Title: reduce</p>  <p>Description: </p> 
		 * 
		 * @param arg0
		 * 
		 * @param arg1
		 * 
		 * @param arg2
		 * 
		 * @throws IOException
		 * 
		 * @throws InterruptedException 
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context) 
		 */

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int class_all = 0;
			int ramCount = 0;
			double tf = 0d;
			Map<String, Integer> map = new HashMap<String, Integer>();
			Set<String> set = null;
			Iterator<String> it = null;
			for (Text val : values) {
				// 计算类目下词的总数：
				class_all += 1;
				// 将分词累加，计算词频率
				if (map.containsKey(val.toString())) {
					map.put(val.toString(), map.get(val.toString()) + 1);
				} else {
					map.put(val.toString(), Integer.valueOf(1));
				}
			}

			// System.out.println(key.toString()+"下总词数："+class_all);
			// 迭代Map计算分词TF
			set = map.keySet();
			it = set.iterator();
			while (it.hasNext()) {
				String ram = it.next();
				ramCount = map.get(ram);
				tf = ramCount * 1.0 / class_all;
				context.write(key, new Text(ram + "\t" + tf));
			}

		}

	}

/*	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CalTFMR");
		job.setJarByClass(com.jd.www.o2o.CalTFMR.class);
		job.setMapperClass(CalTFMapper.class);

		job.setReducerClass(CalTFReducer.class);

		// TODO: specify output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path out = new Path("/user/mart_o2o/tmp.db/TFout");
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(out)) {
			fileSystem.delete(out, true);
		}

		FileInputFormat.setInputPaths(job, new Path("/user/mart_o2o/tmp.db/skuInfo"));
		FileOutputFormat.setOutputPath(job, out);

		if (!job.waitForCompletion(true))
			return;
	}

	public static void main(String[] args) throws Exception {
		CalTFMR caltf = new CalTFMR();
		caltf.run(args);
	}*/

}
