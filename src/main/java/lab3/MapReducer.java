package lab3;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapReducer {
    private final static String _ipPattern = "ip(\\d+)";
    private final static String _payloadSizePattern = "(\".*?\"\\s[^3]\\d\\d\\s)(\\d+)";

    public void mapReduce(JavaRDD<String> logs, String directory) {
        logs.coalesce(1);
        logs.repartition(1);

        logs.mapToPair(line -> {
            Pattern regexp = Pattern.compile(_ipPattern);
            Matcher matcher = regexp.matcher(line);

            if (matcher.find()) {
                String ip = matcher.group(1);

                int bytesCount = 0;
                regexp = Pattern.compile(_payloadSizePattern);
                matcher = regexp.matcher(line);

                if (matcher.find()) {
                    bytesCount = Integer.parseInt(matcher.group(2));
                }
                return new Tuple2<>(ip, new AggregationResult(bytesCount, 1));
            }

            return new Tuple2<>("", new AggregationResult(0, 0));

        }).reduceByKey((current, next) -> {
            current.increment();
            current.addBytesCount(next.getTotalBytesCount());
            return current;
        }).saveAsTextFile(directory);
    }

    public Map<String, Long> analyzeBrowsers(JavaRDD<String> logs) {
        return logs.mapToPair(str -> new Tuple2(UserAgent.parseUserAgentString(str).getBrowser().getGroup().getName(), 0)).countByKey();
    }
}
