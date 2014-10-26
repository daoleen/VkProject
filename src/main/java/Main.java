import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by alex on 12.6.14.
 */
public class Main extends Configured implements Tool {
    private static final Logger logger = Logger.getLogger(Main.class);

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        String[] remainingArgs = new GenericOptionsParser(strings).getRemainingArgs();

        if(configuration == null) {
            configuration = new Configuration(true);
        }

        Job job = new Job(configuration, "Searching BSUIR students");
        job.setJarByClass(Main.class);

        if(remainingArgs.length < 2) {
            System.out.println("Usage: <in directory> <out directory>");
            System.exit(0);
        }

        Path in = new Path(remainingArgs[0]);
        Path out = new Path(remainingArgs[1]);
        File outputDir = new File(remainingArgs[1]);

        if(outputDir.exists()) {
            logger.warn("The output directory is exists. Removing it...");
            FileUtils.deleteDirectory(outputDir);
        }

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(VkMapper.class);
        job.setReducerClass(VkReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(false) ? 0 : 1);
        return 0;
    }


    /**
     * For local debugging
     * @param args
     */
    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }
}
