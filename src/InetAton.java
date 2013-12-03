package com.grooveshark.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

@Description(
    name = "inet_aton",
    value = "_FUNC_(string) - Returns numeric(long/bigint) value of (string)ipaddress",
    extended = "Example:\n  > SELECT inet_aton(ipaddress) FROM usertracking_new WHERE yearweek='t201349';\n"
)

/**
 * Calculation of numeric ipaddress from string
 */
public class InetAton extends UDF {
    public long evaluate(String ipAddress) {
        String[] segments = ipAddress.split("\\.");
        return (long)(Long.parseLong(segments[0]) * 16777216L
                    + Long.parseLong(segments[1]) * 65535L
                    + Long.parseLong(segments[2]) * 256L
                    + Long.parseLong(segments[3])
        );
    }
}
