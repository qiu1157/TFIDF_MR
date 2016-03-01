/**    
* @Title: CalMainMR.java  
* @Package www.jd.com.o2o  
* @Description: TODO(用一句话描述该文件做什么)  
* @author qiuxiangu@gmail.com    
* @date 2016年2月28日 下午7:46:25  
* @version V1.0    
*/

package com.jd.www.o2o;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.jd.www.o2o.SkuInfo.Skuo2oMapper;
import com.jd.www.o2o.SkuInfo.StockMapper;
import com.jd.www.o2o.SkuInfo.skuInfoReducer;
import com.jd.www.o2o.util.Bigram;
import com.jd.www.o2o.CalIDFMR.CalIDFMapper;
import com.jd.www.o2o.CalIDFMR.CalIDFReducer;
import com.jd.www.o2o.CalTFIDFMR.CalTFIDFMapper;
import com.jd.www.o2o.CalTFIDFMR.CalTFIDFReducer;
import com.jd.www.o2o.CalTFMR.CalTFMapper;
import com.jd.www.o2o.CalTFMR.CalTFReducer;

/**
 *  
 * 
 * @ClassName: CalMainMR 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author qiuxiangu@jd.com
 * @date 2016年2月28日 下午7:46:25     
 */

public class CalMainMR {

	private static class CalMainMapper extends Mapper<Object, Text, Text, Text> {

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
			// String[] columns = value.toString().split("\t");
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().getParent().toString();
			if (path.endsWith("skuInfo")) {
				String[] columns = value.toString().split("\t");
				String skuId = columns[0];
				String skuName = columns[1];
				String classId = columns[2];
				String className = columns[3];
				context.write(new Text(classId), new Text(skuId + "\t" + skuName + "\t" + classId + "\t" + className));
			} else if (path.endsWith("TFIDFout")) {
				String[] columns = value.toString().split("\t");
				String classId = columns[0];
				context.write(new Text(classId), value);
			}
		}

	}

	private static class CalMainReducer extends Reducer<Text, Text, Text, Text> {

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
			List<String> list = new ArrayList<String>();
			Map<String, String> map = new HashMap<String, String>();
			Bigram bigram = new Bigram(2);
			// TODO Auto-generated method stub
			for (Text val : values) {
				String[] columns = val.toString().split("\t");
				System.out.println(val.toString());
				if (columns.length == 4) {
					list.add(val.toString());
				} else if (columns.length == 3) {
					map.put(val.toString().split("\t")[1], val.toString().split("\t")[2]);
				}
			}
			
//			Set<String> set = null;
//			Iterator<String> it = null;
//			set = map.keySet();
//			it = set.iterator();
			
			for (int i = 0; i < list.size(); i++) {
				double tfidfSum = 0d;
				String line = list.get(i);
				List<String> rams = bigram.splits(line.split("\t")[1]);
				for (int j = 0; j < rams.size(); j++) {
					String ram = rams.get(j);
					 String tfidf = (map.get(ram)==null ? "0" : map.get(ram));
					 double d = Double.parseDouble(tfidf);
					 tfidfSum += d;
				}
				context.write(new Text(new Date().toLocaleString().substring(0, 9)), new Text(line + "\t" + tfidfSum));
			}
		}
	}

	public void run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
/*		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CalMainMR");
		job.setJarByClass(com.jd.www.o2o.CalMainMR.class);
		job.setMapperClass(CalMainMapper.class);
		job.setReducerClass(CalMainReducer.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// TODO: specify input and output DIRECTORIES (not files)
		Path out =  new Path("/user/mart_o2o/tmp.db/skuout");
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(out)) {
			fileSystem.delete(out, true);
		}
		FileInputFormat.setInputPaths(job, new Path("/user/mart_o2o/tmp.db/skuInfo"));
		FileInputFormat.addInputPath(job, new Path("/user/mart_o2o/tmp.db/TFIDFout"));
		FileOutputFormat.setOutputPath(job, out);

		if (!job.waitForCompletion(true))
			return;
	}*/
	
	//-------------------CalTF的MapReduce作业设置-----------------------------------------
	Configuration calTFConf = new Configuration();
	Job calTFjob = Job.getInstance(calTFConf, "CalTFMR");
	calTFjob.setJarByClass(com.jd.www.o2o.CalTFMR.class);
	calTFjob.setMapperClass(CalTFMapper.class);
	calTFjob.setReducerClass(CalTFReducer.class);

	// TODO: specify output types
	calTFjob.setMapOutputKeyClass(Text.class);
	calTFjob.setMapOutputValueClass(Text.class);
	calTFjob.setOutputKeyClass(Text.class);
	calTFjob.setOutputValueClass(Text.class);

	// TODO: specify input and output DIRECTORIES (not files)
	Path calTFout = new Path("/user/mart_o2o/tmp.db/TFout");
	FileSystem fileSystem = FileSystem.get(calTFConf);
	if (fileSystem.exists(calTFout)) {
		fileSystem.delete(calTFout, true);
	}

	FileInputFormat.setInputPaths(calTFjob, new Path("/user/mart_o2o/tmp.db/skuInfo"));
	FileOutputFormat.setOutputPath(calTFjob, calTFout);
	
	//-------------------CalIDF的MapReduce作业设置-----------------------------------------
	Configuration calIDFConf = new Configuration();
	Job calIDFjob = Job.getInstance(calIDFConf, "CalIDFMR");
	calIDFjob.setJarByClass(com.jd.www.o2o.CalIDFMR.class);

	calIDFjob.setMapperClass(CalIDFMapper.class);

	calIDFjob.setReducerClass(CalIDFReducer.class);

	// TODO: specify output types
	calIDFjob.setOutputKeyClass(Text.class);
	calIDFjob.setOutputValueClass(Text.class);

	// TODO: specify input and output DIRECTORIES (not files)
	Path in = new Path("/user/mart_o2o/tmp.db/skuInfo");
	Path calIDFout = new Path("/user/mart_o2o/tmp.db/IDFout");
	calIDFjob.addCacheFile(new Path("/user/mart_o2o/tmp.db/skuInfo/part-r-00000").toUri());

	fileSystem = FileSystem.get(calIDFConf);
	if (fileSystem.exists(calIDFout)) {
		fileSystem.delete(calIDFout, true);
	}

	FileInputFormat.setInputPaths(calIDFjob, in);
	FileOutputFormat.setOutputPath(calIDFjob, calIDFout);
	
	//-------------------CalTFIDF的MapReduce作业设置-----------------------------------------
	Configuration calTFIDFConf = new Configuration();
	Job calTFIDFjob = Job.getInstance(calTFIDFConf, "CalTFIDFMR");
	calTFIDFjob.setJarByClass(com.jd.www.o2o.CalTFIDFMR.class);
	calTFIDFjob.setMapperClass(CalTFIDFMapper.class);

	calTFIDFjob.setReducerClass(CalTFIDFReducer.class);

	// TODO: specify output types
	calTFIDFjob.setOutputKeyClass(Text.class);
	calTFIDFjob.setOutputValueClass(Text.class);

	// TODO: specify input and output DIRECTORIES (not files)
	Path calTFIDFout = new Path("/user/mart_o2o/tmp.db/TFIDFout");
	fileSystem = FileSystem.get(calTFIDFConf);
	if (fileSystem.exists(calTFIDFout)) {
		fileSystem.delete(calTFIDFout, true);
	}
	FileInputFormat.setInputPaths(calTFIDFjob, new Path("/user/mart_o2o/tmp.db/TFout"));
	FileInputFormat.addInputPath(calTFIDFjob, new Path("/user/mart_o2o/tmp.db/IDFout"));
	FileOutputFormat.setOutputPath(calTFIDFjob, calTFIDFout);	
	
	//-------------------CalMain的MapReduce作业设置-----------------------------------------
	Configuration calMainConf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(calMainConf, args).getRemainingArgs();
	String dealDateStr = calMainConf.get("dealDate");
	Job calMainjob = Job.getInstance(calMainConf, "CalMainMR");
	calMainjob.setJarByClass(com.jd.www.o2o.CalMainMR.class);
	calMainjob.setMapperClass(CalMainMapper.class);
	calMainjob.setReducerClass(CalMainReducer.class);
	// TODO: specify output types
	calMainjob.setOutputKeyClass(Text.class);
	calMainjob.setOutputValueClass(Text.class);
	// TODO: specify input and output DIRECTORIES (not files)
	Path out =  new Path("/user/mart_o2o/tmp.db/skuout");
	fileSystem = FileSystem.get(calMainConf);
	if (fileSystem.exists(out)) {
		fileSystem.delete(out, true);
	}
	FileInputFormat.setInputPaths(calMainjob, new Path("/user/mart_o2o/tmp.db/skuInfo"));
	FileInputFormat.addInputPath(calMainjob, new Path("/user/mart_o2o/tmp.db/TFIDFout"));
	FileOutputFormat.setOutputPath(calMainjob, out);	

	//-------------------skuInfo的MapReduce作业设置-----------------------------------------
	Configuration skuInfoConf = new Configuration();
	Job skuInfoJob = Job.getInstance(skuInfoConf, "skuInfo");
	skuInfoJob.setJarByClass(com.jd.www.o2o.SkuInfo.class);
	skuInfoJob.setMapperClass(Skuo2oMapper.class);
	skuInfoJob.setMapperClass(StockMapper.class);
	// TODO: specify a reducer
	skuInfoJob.setReducerClass(skuInfoReducer.class);
	// TODO: specify output types
	skuInfoJob.setInputFormatClass(LzoTextInputFormat.class);
	skuInfoJob.setOutputKeyClass(Text.class);
	skuInfoJob.setOutputValueClass(Text.class);
	skuInfoJob.setOutputFormatClass(TextOutputFormat.class);
	
	// TODO: specify input and output DIRECTORIES (not files)
	MultipleInputs.addInputPath(skuInfoJob, new Path("/user/dd_edw/gdm.db/gdm_m03_item_sku_o2o_da/dt="+dealDateStr), LzoTextInputFormat.class, Skuo2oMapper.class);
	MultipleInputs.addInputPath(skuInfoJob, new Path("/user/dd_edw/fdm.db/fdm_stock_center_stock_center_chain/dp=ACTIVE/dt=4712-12-31/end_date=4712-12-31/"), LzoTextInputFormat.class, StockMapper.class);
	Path skuInfoout = new Path("/user/mart_o2o/tmp.db/skuInfo");
	fileSystem = FileSystem.get(skuInfoConf); 
	if (fileSystem.exists(skuInfoout)) {
		fileSystem.delete(skuInfoout, true);
	}
	FileOutputFormat.setOutputPath(skuInfoJob, skuInfoout);
	
	
	ControlledJob skuinfoJobCtr = new ControlledJob(skuInfoConf);
	ControlledJob calTFJobCtr = new ControlledJob(calTFConf);
	ControlledJob calIDFJobCtr = new ControlledJob(calIDFConf);
	ControlledJob calTFIDFJobCtr = new ControlledJob(calTFIDFConf);
	ControlledJob calMainJobCtr = new ControlledJob(calMainConf);
	
	calTFJobCtr.addDependingJob(skuinfoJobCtr);
	calIDFJobCtr.addDependingJob(skuinfoJobCtr);
	calTFIDFJobCtr.addDependingJob(calTFJobCtr);
	calTFIDFJobCtr.addDependingJob(calIDFJobCtr);
	calMainJobCtr.addDependingJob(calTFIDFJobCtr);
	
	JobControl jobControl = new JobControl("sku_class");
	jobControl.addJob(skuinfoJobCtr);
	jobControl.addJob(calTFJobCtr);
	jobControl.addJob(calIDFJobCtr);
	jobControl.addJob(calTFIDFJobCtr);
	jobControl.addJob(calMainJobCtr);
	
	skuinfoJobCtr.setJob(skuInfoJob);
	calTFJobCtr.setJob(calTFjob);
	calIDFJobCtr.setJob(calIDFjob);
	calTFIDFJobCtr.setJob(calTFIDFjob);
	calMainJobCtr.setJob(calMainjob);
	
	Thread jobControlThread =new Thread(jobControl);
	jobControlThread.start();
	while(!jobControl.allFinished()) {
	Thread.sleep(500);
	}
	jobControl.stop();
	return;
	}
	public static void main(String[] args) throws Exception {
		CalMainMR calMain = new CalMainMR();
		calMain.run(args);
	}

}
