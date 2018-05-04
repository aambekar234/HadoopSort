import java.util.*; 
import java.io.IOException; 
import java.io.IOException; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.logging.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Partitioner;

public class HadoopSort extends Configured implements Tool
{ 
   //Mapper class 
   public static class MyMapper extends Mapper<Text,Text,Text,Text>  
   { 
      
      public void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException 
      { 
	try
	{
		//pass key value pair
         	context.write(key,value);
	}
	catch(Exception e)
	{
	}

      } 
   } 
   
   
   public static class MyReducer extends Reducer< Text, Text, Text, Text > 
   {  
   
      public void reduce( Text fromMapperKey, Iterable<Text> fromReducerValue, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException 
         { 
		try
		{
			//write to output
			context.write(fromMapperKey, fromReducerValue);
		}
		catch(Exception e)
		{
		}
            
 
         } 
   }  

public static class KeyPartitioner extends Partitioner<Text, Text>
{
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
       
        if(numReduceTasks == 0)
        {
            return 0;
        }
	String c = key.toString();
	int tmp = (int)c.charAt(0);
	int block = 127/numReduceTasks;
	for(int i = 0;i<numReduceTasks;i++)
	{
		if(tmp < ((i+1) * block))
		{
			return i;
		}
	}


        return numReduceTasks-1;
        
    }
}

 public static void main( String[] args )
    {
    	int res;
		try 
		{
			res = ToolRunner.run(new Configuration(), new HadoopSort(), args);
			 System.exit(res);  
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
          
    }
   
   
   //Main function 
   public int run(String args[])throws Exception 
   { 
      		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HadoopSort");

		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress", "false");
		Long start = System.currentTimeMillis();
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(80);
		job.setJarByClass(HadoopSort.class);
		job.setPartitionerClass(KeyPartitioner.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
	

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		
		Long end = System.currentTimeMillis();
		

		Logger.getLogger(HadoopSort.class.getName()).info("**** Time took to sort : " + (end - start)/1000 +" seconds ****");
		
  		return 0;    		
   } 
} 
