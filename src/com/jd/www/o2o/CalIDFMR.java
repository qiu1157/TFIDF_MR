/**    
* @Title: CalIDFMR.java  
* @Package www.jd.com.o2o  
* @Description: TODO(用一句话描述该文件做什么)  
* @author qiuxiangu@gmail.com    
* @date 2016年2月28日 上午11:37:56  
* @version V1.0    
*/

package com.jd.www.o2o;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * @ClassName: CalIDFMR 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author qiuxiangu@jd.com
 * @date 2016年2月28日 上午11:37:56     
 */

public class CalIDFMR {

	private static class CalIDFMapper extends Mapper<Object, Text, Text, Text> {
		private Bigram bigram;
		Set<String> set = new HashSet<String>();
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
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			bigram = new Bigram(2);
			BufferedReader in = null;
			String line = null;
			Path[] paths = context.getLocalCacheFiles();
			try {
				if (null != paths) {
					for (Path path : paths) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (line = in.readLine())) {
							set.add(line.split(",")[2]);
						}
					}
				}
			} finally {
				// TODO: handle finally clause
				if (null != in) {
					in.close();
				}
			}
			System.out.println("set 大小=="+ set.size());
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
			String[] columns = value.toString().split(",");
			String skuName = columns[1].replace("\t", " ");
			;
			String classId = columns[2];
			List<String> ram = bigram.splits(skuName);
			for (int i = 0; i < ram.size(); i++) {
				context.write(new Text(ram.get(i)), new Text(classId));
				context.write(new Text(ram.get(i)), new Text("allClass," + set.size()));
			}
		}

	};

	private static class CalIDFReducer extends Reducer<Text, Text, Text, Text> {

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
			// 总文档数
			int allClass = 0;
			int ramCount = 0;
			for (Text val : values) {
				if (val.toString().startsWith("allClass")) {
					allClass = Integer.valueOf(val.toString().split(",")[1]);
				} else {
					ramCount += 1;
				}
			}
			System.out.println("总class数=="+allClass);
			//System.out.println(key.toString()+"出现的在类目下=="+ramCount);
			double idf = Math.log10(allClass * 1.0 / (1+ramCount * 1.0));
			context.write(key, new Text(""+idf));
		}

	};

	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CalIDFMR");
		job.setJarByClass(com.jd.www.o2o.CalIDFMR.class);

		job.setMapperClass(CalIDFMapper.class);

		job.setReducerClass(CalIDFReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path in = new Path("hdfs://master.hadoop:9000/skuin");
		Path out = new Path("hdfs://master.hadoop:9000/IDFout");
		job.addCacheFile(new Path("/skuin/20795.txt").toUri());
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		if (!job.waitForCompletion(true))
			return;
	}

	public static void main(String[] args) throws Exception {
		CalIDFMR calIDF = new CalIDFMR();
		calIDF.run(args);
	}

}
