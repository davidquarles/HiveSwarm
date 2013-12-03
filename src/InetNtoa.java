package com.grooveshark.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

import java.net.*;

@Description(
    name = "inet_ntoa",
    value = "_FUNC_(string) - Returns string value of (numeric) ipaddress",
    extended = "Example:\n  > SELECT inet_ntoa(ipaddress) FROM usertracking_archived WHERE datecomputed = '2013-12-03';\n"
)

/**
 * Calculation of String IP Address, given numeric value
 */
public class InetNtoa extends UDF {
    public String evaluate(long raw) {
        byte[] b = new byte[] {(byte)(raw >> 24), (byte)(raw >> 16), (byte)(raw >> 8), (byte)raw};
        try {
            return InetAddress.getByAddress(b).getHostAddress();
        } catch (UnknownHostException e) {
            // No way here
            return null;
        }
    }
}


