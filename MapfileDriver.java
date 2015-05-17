import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapfileDriver {
	public static void main(String args[]) throws Exception
	{
		if(args.length!=2)
			{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
			}
		Path outfile=new Path(args[1]);
		Path infile=new Path(args[0]);
		Configuration conf=new Configuration();
		conf.set("outputfile1",outfile.toString());
		conf.set("inputfile1",infile.toString());
		Job job=new Job(conf);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
	


		conf.set("test", "check");
		job.setJarByClass(MapfileDriver.class);
		job.setJobName("MapfileDriver");
		

		job.setMapperClass(MapfileMapper.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
