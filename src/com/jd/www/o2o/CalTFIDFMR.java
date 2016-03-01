/**    
* @Title: CalTFIDFMR.java  
* @Package www.jd.com.o2o  
* @Description: TODO(用一句话描述该文件做什么)  
* @author qiuxiangu@gmail.com    
* @date 2016年2月28日 下午6:57:11  
* @version V1.0    
*/

package com.jd.www.o2o;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**  
* @ClassName: CalTFIDFMR  
* @Description: TODO(这里用一句话描述这个类的作用)  
* @author qiuxiangu@jd.com 
* @date 2016年2月28日 下午6:57:11  
*    
*/

public class CalTFIDFMR {
	
	public static class CalTFIDFMapper extends Mapper<Object, Text, Text, Text> {

		/* (非 Javadoc)  
		* <p>Title: map</p>  
		* <p>Description: </p>  
		* @param key
		* @param value
		* @param context
		* @throws IOException
		* @throws InterruptedException  
		* @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)  
		*/
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] columns = value.toString().split("\t");
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String path = fileSplit.getPath().getParent().toString();
			if (path.endsWith("TFout")) {
				String classId = columns[0];
				String ram = columns[1];
				String tf = columns[2];
				context.write(new Text(ram), new Text(classId+"\t"+tf));
			}else if (path.endsWith("IDFout")) {
				String ram = columns[0];
				String idf = columns[1];
				context.write(new Text(ram), new Text(idf));
			}
		}
		
	}
	
	public static class CalTFIDFReducer extends Reducer<Text, Text, Text, Text> {

		/* (非 Javadoc)  
		* <p>Title: reduce</p>  
		* <p>Description: </p>  
		* @param arg0
		* @param arg1
		* @param arg2
		* @throws IOException
		* @throws InterruptedException  
		* @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)  
		*/
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double tf = 0d;
			double idf = 0d;
			String classId = null ;
			for (Text val : values) {
				String[] str = val.toString().split("\t");
				if (str.length > 1) {
					classId = str[0];
					tf = Double.parseDouble(str[1]);
				}else {
					idf = Double.parseDouble(str[0]);
				}
			}
			double tfidf = tf * idf;
			context.write(new Text(classId), new Text(key.toString()+"\t"+tfidf));
		}
		
	}
	
/*	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CalTFIDFMR");
		job.setJarByClass(com.jd.www.o2o.CalTFIDFMR.class);
		job.setMapperClass(CalTFIDFMapper.class);

		job.setReducerClass(CalTFIDFReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path out = new Path("/user/mart_o2o/tmp.db/TFIDFout");
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(out)) {
			fileSystem.delete(out, true);
		}
		FileInputFormat.setInputPaths(job, new Path("/user/mart_o2o/tmp.db/TFout"));
		FileInputFormat.addInputPath(job, new Path("/user/mart_o2o/tmp.db/IDFout"));
		FileOutputFormat.setOutputPath(job, out);

		if (!job.waitForCompletion(true))
			return;		
	}
	public static void main(String[] args) throws Exception {
		CalTFIDFMR calTFIDF = new CalTFIDFMR();
		calTFIDF.run(args);
	}
*/
}
