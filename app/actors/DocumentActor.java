package actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import config.Configuration;
import org.json.JSONException;
import org.json.JSONObject;
import play.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

public class DocumentActor extends AbstractActor
{
    private static final Logger.ALogger LOGGER = Logger.of(DocumentActor.class);
    private static final String DELETE_TO_QUERY_ADDRESS = Configuration.ELASTIC_ADDRESS + "/testdocs/_delete_by_query";
//    private static final String SEARCH_ADDRESS = Configuration.ELASTIC_ADDRESS + "/_search";

    public static Props getProps()
    {
        return Props.create(DocumentActor.class);
    }

    @Override
    public Receive createReceive()
    {
//        LOGGER.warn("DocumentActor Receive Method...");
        return ReceiveBuilder.create().match(
                String.class, message -> {

                    LOGGER.warn("DocumentActorActor recieved: " + message);

                    if(message.equalsIgnoreCase("RemoveDocuments"))
                    {
                        LOGGER.warn("Removing documents...");
                        int count = removeDocuments();
                        LOGGER.warn("Removed documents: " + count);
                    }

                    String response = "Success!";
                    sender().tell(response, self());
                }).build();
    }


    private int removeDocuments()
    {
        try
        {
            URLConnection connection = new URL(DELETE_TO_QUERY_ADDRESS).openConnection();
            HttpURLConnection httpURLConnection = (HttpURLConnection) connection;
            httpURLConnection.setRequestMethod("POST");
            httpURLConnection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
            httpURLConnection.setRequestProperty("Accept", "application/json");
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setDoInput(true);

            String jsonParam = "{ \"query\" : { \"match\" : { \"shouldDelete\" : true}}}";
            byte[] out = jsonParam.getBytes(StandardCharsets.UTF_8);
            int length = out.length;

            httpURLConnection.setFixedLengthStreamingMode(length);
            httpURLConnection.connect();

            try(OutputStream outputStream = httpURLConnection.getOutputStream())
            {
                outputStream.write(out);
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream()));
            String line;
            StringBuilder result = new StringBuilder();
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            rd.close();

            JSONObject responseJson = new JSONObject(result.toString());
            if(responseJson.has("deleted"))
            {
                return responseJson.getInt("deleted");
            }
        }
        catch(IOException | JSONException e)
        {
            e.printStackTrace();
        }

        return 0;
    }
}
