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

public class adjList { 

  public static class adjMapper
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


  public static class adjReducer
      extends Reducer<Text,Text,Text, Text> {
    private Text result = new Text();
	private Text counter= new Text();
	     
    public static int maxcount = 0;
    public static int mincount = 0;
    public static String adjlstfinal = "";
    Text finalkey = new Text();
    Text finalkeymin = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cntr = 0, fincnt = 0;
      Text temp = new Text();  
      String adjlst = "";
       
      System.out.println("enter reducer" +key);
      for (Text val : values)
     
      {
	  adjlst = adjlst+","+val;
	  cntr = cntr + 1;                    
      }

      
      adjlst = adjlst.substring(1);
	  //adjlst = key + "#" +adjlst;
       adjlst = cntr + "\t" + adjlst;
      fincnt = cntr;
	  result.set(adjlst);
	  //counter.set(fincnt.toString());
      	  //String s= Integer.toString(fincnt);
      //context.write(new Text(s),result);
      context.write(key,result);

     
        
}

/*@Override

 

protected void cleanup(Context context) throws IOException, InterruptedException {
context.write(finalkey, new Text(adjlstfinal)); 
context.write(finalkeymin, new Text("Node with min Adj List"));
} */     
       
      
}



  public static void main(String[] args) throws Exception {
    
    System.out.println("start main souseelya");
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Cloud Computing Adj List");
    job.setJarByClass(adjList.class);
    job.setMapperClass(adjMapper.class);
    //job.setCombinerClass(adjReducer.class);
    job.setReducerClass(adjReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));    
    job.waitForCompletion(true);
    String cmd = "sh /home/kidambsy/minmax.sh";
    Process process = Runtime.getRuntime().exec(cmd);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    System.out.println("end main souseelya");
  }
}

