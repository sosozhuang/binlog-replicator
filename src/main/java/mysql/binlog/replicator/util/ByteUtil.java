package mysql.binlog.replicator.util;

/**
 * @author zhuangshuo
 */
public final class ByteUtil {
    private ByteUtil() {
        throw new RuntimeException();
    }

    /**
     * Convert long array to byte array.
     *
     * @param values long array.
     * @return converted byte array.
     */
    public static byte[] longsToBytes(long... values) {
        int len = values.length;
        byte[] result = new byte[len << 3];
        int o;
        for (int i = 0; i < len; ++i) {
            o = i << 3;
            for (int j = o + 7; j >= o; --j) {
                result[j] = (byte) ((values[i] & 0xff));
                values[i] >>= 8;
            }
        }
        return result;
    }

    /**
     * Convert byte array to long array.
     *
     * @param bytes byte array.
     * @return converted long array.
     */
    public static long[] bytesToLongs(byte[] bytes) {
        long[] result = new long[bytes.length >> 3];
        int o;
        for (int i = 0, len = result.length; i < len; ++i) {
            o = i << 3;
            result[i] = ((long) bytes[o] & 0xff) << 56
                    | ((long) bytes[1 + o] & 0xff) << 48
                    | ((long) bytes[2 + o] & 0xff) << 40
                    | ((long) bytes[3 + o] & 0xff) << 32
                    | ((long) bytes[4 + o] & 0xff) << 24
                    | ((long) bytes[5 + o] & 0xff) << 16
                    | ((long) bytes[6 + o] & 0xff) << 8
                    | (long) bytes[7 + o] & 0xff;
        }
        return result;
    }
}
