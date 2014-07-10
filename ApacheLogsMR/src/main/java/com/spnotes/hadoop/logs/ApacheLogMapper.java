package com.spnotes.hadoop.logs;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gpzpati on 7/8/14.
 */
public class ApacheLogMapper extends Mapper<LongWritable,Text, Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(ApacheLogReducer.class);

    Pattern logPattern;
    DatabaseReader reader;
    private static final int NUM_FIELDS =9;
    private final static IntWritable one = new IntWritable(1);
    private Text clientIp = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Including changes for GeoLite2-city.mmdb");
        String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
        logPattern = Pattern.compile(logEntryPattern);
        URI[] cacheFileURI = context.getCacheFiles();
        for(URI uri: cacheFileURI){
            System.out.println(uri);
        }
        File file = new File("GeoLite2-City.mmdb");
        reader = new DatabaseReader.Builder(file).build();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.debug("Entering ApacheLogMapper.map()");
        Matcher matcher = logPattern.matcher(value.toString());
        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            System.out.println("Unable to parse input");
        }else{
            String ip = matcher.group(1);
            try {
                CityResponse response = reader.city(InetAddress.getByName(ip));
                StringBuffer city = new StringBuffer();
                String cityName = response.getCity().getName();
                String countryName = response.getCountry().getName();
                if(cityName != null && cityName.length() != 0){
                    city.append(cityName);
                    city.append(", ");
                }
                if(countryName != null && countryName.length() != 0){
                    city.append(countryName);
                }

                clientIp.set(city.toString());
                context.write(clientIp, one);
            }catch(GeoIp2Exception ex){
                logger.error("Error in getting address ", ex);
            }

        }
        logger.debug("Exiting ApacheLogMapper.map()");

    }
}
