import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BigramCountMapper extends
        Mapper<Object, Text, Bigrama, IntWritable> {

    private final IntWritable ONE = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\\s+");
        this.emitBigram("", tokens[0], context);
        for (int i = 0; i < tokens.length - 1; i++) {
            this.emitBigram(tokens[i], tokens[i + 1], context);
        }
        this.emitBigram(tokens[tokens.length - 1], "", context);
    }

    private void emitBigram(String a, String b, Context context) throws IOException, InterruptedException {
        Bigrama bigram = new Bigrama(a, b);
        context.write(bigram, ONE);
    }
}
