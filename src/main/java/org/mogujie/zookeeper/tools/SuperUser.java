package org.mogujie.zookeeper.tools;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.zookeeper.KeeperException;

public class SuperUser {

    public static void main(String[] args) throws IOException,
            InterruptedException, KeeperException {
        String passwd = "super:guangming";
        System.out.println(generateDigest(passwd));
    }

    static public String generateDigest(String idPassword) {
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

    static final private String base64Encode(byte b[]) {
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

    static final private char encode(int i) {
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