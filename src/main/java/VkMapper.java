import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.internal.bind.JsonTreeReader;
import com.google.gson.stream.JsonReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by alex on 12.6.14.
 */
public class VkMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private final static Logger logger = Logger.getLogger(VkMapper.class);
    private final Gson gson = new Gson();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.debug("Key is: " + key);
        logger.debug("Value is: " + value);

        try {
            JSONObject response = new JSONObject(value.toString());
            JSONArray users = response.getJSONArray("response");

            for(int i = 0; i < users.length(); i++) {
                JSONObject user = users.getJSONObject(i);
                logger.debug("User is: " + user);

                long id = Long.parseLong(user.get("id").toString());
                logger.debug("User id is: " + id);

                try {
                    JSONArray universities = user.getJSONArray("universities");

                    if (universities != null) {
                        for (int j = 0; j < universities.length(); j++) {
                            JSONObject university = universities.getJSONObject(j);
                            long universityId = 0;
                            long facultyId = 0;
                            logger.debug("University is: " + university);

                            try {
                                universityId = Long.parseLong(university.get("id").toString());
                                facultyId = Long.parseLong(university.get("faculty").toString());
                            }
                            catch (NumberFormatException ex) {
                                logger.error("Couldn't parse an university id");
                                logger.error("University is: " + university);
                            }

                            if(universityId == 94448 || universityId == 169688) {
                                context.write(new LongWritable(facultyId), new Text(user.toString()));
                            }
                        }
                    }
                }
                // Probably, couldn't found the university
                catch (JSONException ex) {
                    logger.warn(ex);
                }
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }


    private void oldMap(String value) throws IOException {
        JsonReader reader = new JsonReader(new StringReader(value.toString()));
        reader.setLenient(true);

        // Response object
        reader.beginObject();
        String responseName = reader.nextName();    // response
        logger.debug("ResponseName is: " + responseName);

        // Array of users
        reader.beginArray();

        // Iterator by users
        while(reader.hasNext()) {

            // User object
            reader.beginObject();

            // Moving by user properties
            while (reader.hasNext()) {
                boolean bsuir = false;
                String property = reader.nextName();
                logger.debug("Property is: " + property);

                if(property.equals("universities")) {

                    // array of universities
                    reader.beginArray();

                    // iterator by universities
                    while(reader.hasNext()) {

                        // university object
                        reader.beginObject();
                        if(reader.nextName().equals("id")) {
                            int universityId = reader.nextInt();

                            if(universityId == 94448 || universityId == 169688) {
                                bsuir = true;
                            }
                        }
                        reader.endObject();

                    }

                    reader.endArray();


                    int university = reader.nextInt();
                }

                String someProperty = reader.nextString();
                logger.debug("SomeProperty is: " + someProperty);
            }
            reader.endObject();

        }

        reader.endArray();

        reader.endObject();
    }
}
