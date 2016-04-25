import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
                String[] coordinates = key.toString().split(" ");
		double a = Double.parseDouble(columns[0]);
		double b= Double.parseDouble(columns[1]);

		int y = 0;
		//int sumXY = 0;
		//int sumY = 0;
		//int sumX2 = 0;
		//int size = 0;
		
		for(Text value : values){
			String[] tmpValues = value.toString().split(" ");
			String timeStamp = tmpValue[0];
			double pmReading = Double.parseDouble(tmpValue[1]);

			sumX += timeStamp;
			sumXY += pmReading * timeStamp;
			sumY += pmReading;
			sumX2 += timeStamp^2;
			size++;
		}
		y		
		context.write(new Text(a + " " + b), new Text( y ));
	}
}
