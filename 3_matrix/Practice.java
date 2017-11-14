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


public class Practice{

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	StringTokenizer itr = new StringTokenizer(value.toString(), " ,[]\""); 
	String matrix = itr.nextToken();		// word = "a" or "b"
	int row = Integer.parseInt(itr.nextToken());
	int col = Integer.parseInt(itr.nextToken());
        if(matrix.contains("a")){
		for(int i = 0; i < 5; i++){
			String akey = Integer.toString(row) + "," + Integer.toString(i);
			Text AMkey = new Text(akey);
			context.write(AMkey, value);
		}
	}
	else if(matrix.contains("b")){
		for(int i = 0; i < 5; i++){
			String bkey = Integer.toString(i) + "," + Integer.toString(col);
			Text BMkey = new Text(bkey);
			context.write(BMkey, value);
		}
	}
    }
  }
  

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text totalText = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	int[] a = {0,0,0,0,0};
	int[] b = {0,0,0,0,0};
	System.out.print(key + "\t");	
	for (Text val : values) {
		String[] cell = val.toString().split(",");
		System.out.print(val + " ");
		if(cell.length >= 3){
			String[] dataCell = cell[3].split("]");
			//System.out.print(cell[3]);
			String matrix = cell[0];
			int row1 = Integer.parseInt(cell[1].trim());
			int col1 = Integer.parseInt(cell[2].trim());
			int data = Integer.parseInt(dataCell[0].trim());
			if(matrix.contains("a"))
				a[col1] = data;
			else
				b[row1] = data;
		}
	}
	int total = 0;
	for(int i = 0; i < 5; i++){
		int mult = a[i] * b[i];
		total += mult;
	}
	totalText.set(String.valueOf(total));
	System.out.println(totalText + "\n");
	context.write(key, totalText);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Practice 20170929");
    job.setJarByClass(Practice.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
