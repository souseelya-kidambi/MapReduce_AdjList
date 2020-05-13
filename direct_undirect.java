import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class direct_undirect { 

  public static class duMapper
       extends Mapper<Object, Text, Text, Text>{

    // private final static IntWritable one = new IntWritable(1);
    private Text outVal = new Text();
      private Text outKey = new Text();
     private Text outVal1 = new Text();
      private Text outKey1 = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
	String inline = value.toString();
        
	if (!inline.startsWith("#"))
	{
	    String [] inVals = inline.split("\t");
            if(inVals[1].length()>0)
            {
	    outKey.set(inVals[0]);
	    outVal.set(inVals[1]);
            outKey1.set(inVals[1]);
	    outVal1.set(inVals[0]);
            context.write(outKey, outVal);
            context.write(outKey1, outVal1);
            }
	}
        
    }
  }


   public static class duReducer
      extends Reducer<Text,Text,Text, Text> {
     public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
            for (Text val : values)
     
      {
	    context.write(key,val);                    
      }
     
} 
       
      
}



  public static void main(String[] args) throws Exception {
    
    System.out.println("start main souseelya");
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Cloud Computing Adj List");
    job.setJarByClass(direct_undirect.class);
    job.setMapperClass(duMapper.class);
    //job.setCombinerClass(adjReducer.class);
    job.setReducerClass(duReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    System.out.println("end main souseelya");
  }
}

