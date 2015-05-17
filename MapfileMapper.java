import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapfileMapper extends Mapper<LongWritable,Text,IntWritable,Text> {
	private String outputfile;
	private String inputfile2;
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		MapFile.Writer writer=null;
		Configuration conf1=new Configuration();
		
		Path inputfile=new Path(inputfile2);
		FileSystem fs=FileSystem.get(conf1);
		
		try
		{
		writer= new MapFile.Writer(conf1, fs, outputfile, key.getClass(), value.getClass());
		FSDataInputStream in=fs.open(inputfile);
		while(in.available() > 0)
		{
		@SuppressWarnings("deprecation")
		String value1=in.readLine();
		String s=value1.toString();
		String s1[]=s.split(",");
		writer.append(new IntWritable(Integer.parseInt(s1[0])),	 new Text(new Text(s1[1])));
		}
		}
		finally
		{
			IOUtils.closeStream(writer);
			fs.close();
		}
	}
	public void setup(Context context) throws IOException
	{
		Configuration conf=context.getConfiguration();
		outputfile =conf.get("outputfile1");
		inputfile2 =conf.get("inputfile1");
	}
	public void run(Context context) throws IOException, InterruptedException 
	{
		setup(context);
			
		while(context.nextKeyValue())
		{
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
	}
}
