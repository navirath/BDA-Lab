import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> map = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        map.put(key.toString(), sum); // store all counts
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // ✅ Convert map to list
        List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());

        // ✅ Sort descending by value
        list.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // ✅ Output top 10
        int count = 0;
        for (Map.Entry<String, Integer> entry : list) {
            if (count == 10) break;

            context.write(new Text(entry.getKey()),
                          new IntWritable(entry.getValue()));

            count++;
        }
    }
}
