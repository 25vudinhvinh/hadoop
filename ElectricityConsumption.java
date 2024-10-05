import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Mapper class
public class ElectricityConsumption {

    public static class ElectricityMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Chia dòng dữ liệu đầu vào thành các phần tử
            String[] parts = value.toString().split("\\s+");

            // Giả sử cột đầu là năm và cột cuối là mức tiêu thụ trung bình
            String year = parts[0]; // lấy năm
            int avgConsumption = Integer.parseInt(parts[13]); // lấy cột "Avg"

            // Xuất cặp (key, value) là (năm, giá trị tiêu thụ trung bình)
            context.write(new Text(year), new IntWritable(avgConsumption));
        }
    }

    // Reducer class
    public static class ElectricityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if (val.get() > 30) {
                    // Xuất ra năm và giá trị tiêu thụ điện trung bình nếu lớn hơn 30
                    context.write(key, val);
                }
            }
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "electricity consumption");

        // Chỉ định class chính
        job.setJarByClass(ElectricityConsumption.class);

        // Chỉ định Mapper và Reducer
        job.setMapperClass(ElectricityMapper.class);
        job.setReducerClass(ElectricityReducer.class);

        // Đầu ra của Mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Đường dẫn đầu vào và đầu ra
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Chạy job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}









