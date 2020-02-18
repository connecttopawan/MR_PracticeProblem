import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.mapper;
import org.apache.hadoop.mapreduce.reducer;
import java.io.IOException;
public class WordCount
{
// Mapper class defination
public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
// splitting input lines into words
String words[]=value.toString().trim().split(",");
// Assigning 1 to each word and creating key value pair.
for(String x: words)
{
context.write(new Text(x),new IntWritable(1);
}
}
}
// Reducer class reducer
public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
{
int sum=0;
for (Intwritable x: values)
{
sum=sum+value.get();
}
context.write(key, new IntWritable(sum));
}
}
}
