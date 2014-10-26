import com.cenqua.clover.reporters.html.JSONObjectFactory;
import com.cenqua.clover.reporters.json.JSONStringer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Created by alex on 12.6.14.
 */
public class VkReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
    private final static Logger logger = Logger.getLogger(VkReducer.class);

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        logger.debug("In the reducer");
        logger.debug("Reducer key: " + key);
        logger.debug("Reducer values: " + values);

        try {
            JSONObject faculty = new JSONObject(String.format("{%s:[]}", key.toString()));
            JSONArray students = faculty.getJSONArray(key.toString());

            for(Text value : values) {
                students.put(new JSONObject(value.toString()));
            }

            logger.debug("Faculty is: " + faculty);
            context.write(null, new Text(faculty.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
