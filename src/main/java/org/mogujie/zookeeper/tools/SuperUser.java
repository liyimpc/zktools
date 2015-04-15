package org.mogujie.zookeeper.tools;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SuperUser {

    public static void main(String[] args) throws IOException,
            InterruptedException {
        String passwd = "super:guangming";
        if (!parseArgs(args)) {
          System.err.println("error argument!");
          System.err.println("please use: sh superUser.sh super:XXXX");
        } else {
          passwd = args[0];
          System.out.println(generateDigest(passwd));
        }
    }

    private static boolean parseArgs(String[] args) {
      if (args.length != 1) {
        return false;
      }
      if (!args[0].startsWith("super")) {
        return false;
      }
      return true;
    }

    public static String generateDigest(String idPassword) {
        String parts[] = idPassword.split(":", 2);
        byte digest[] = null;
        try {
            digest = MessageDigest.getInstance("SHA1").digest(
                    idPassword.getBytes());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return parts[0] + ":" + base64Encode(digest);
    }

    private static final String base64Encode(byte b[]) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.length;) {
            int pad = 0;
            int v = (b[i++] & 0xff) << 16;
            if (i < b.length) {
                v |= (b[i++] & 0xff) << 8;
            } else {
                pad++;
            }
            if (i < b.length) {
                v |= (b[i++] & 0xff);
            } else {
                pad++;
            }
            sb.append(encode(v >> 18));
            sb.append(encode(v >> 12));
            if (pad < 2) {
                sb.append(encode(v >> 6));
            } else {
                sb.append('=');
            }
            if (pad < 1) {
                sb.append(encode(v));
            } else {
                sb.append('=');
            }
        }
        return sb.toString();
    }

    private static final char encode(int i) {
        i &= 0x3f;
        if (i < 26) {
            return (char) ('A' + i);
        }
        if (i < 52) {
            return (char) ('a' + i - 26);
        }
        if (i < 62) {
            return (char) ('0' + i - 52);
        }
        return i == 62 ? '+' : '/';
    }
}