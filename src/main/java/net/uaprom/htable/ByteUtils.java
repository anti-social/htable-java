package net.uaprom.htable;

import java.util.Collection;


public class ByteUtils {
    public static int getMinimumNumberOfBytes(int value) {
        int highestSetBit = 32 - Integer.numberOfLeadingZeros(value);
        if (highestSetBit == 0) {
            return 1;
        }
        return highestSetBit / 8 + (highestSetBit % 8 == 0 ? 0 : 1);
    }

    public static int getMinimumNumberOfBytes(long value) {
        int highestSetBit = 64 - Long.numberOfLeadingZeros(value);
        if (highestSetBit == 0) {
            return 1;
        }
        return highestSetBit / 8 + (highestSetBit % 8 == 0 ? 0 : 1);
    }

    public static byte[] toByteArray(Collection<Byte> values) {
        byte[] array = new byte[values.size()];
        int i = 0;
        for (byte v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static byte[][] toBytesArray(Collection<byte[]> values) {
        byte[][] array = new byte[values.size()][];
        int i = 0;
        for (byte[] v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static short[] toShortArray(Collection<Short> values) {
        short[] array = new short[values.size()];
        int i = 0;
        for (short v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static int[] toIntArray(Collection<Integer> values) {
        int[] array = new int[values.size()];
        int i = 0;
        for (int v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static long[] toLongArray(Collection<Long> values) {
        long[] array = new long[values.size()];
        int i = 0;
        for (long v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static float[] toFloatArray(Collection<Float> values) {
        float[] array = new float[values.size()];
        int i = 0;
        for (float v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static double[] toDoubleArray(Collection<Double> values) {
        double[] array = new double[values.size()];
        int i = 0;
        for (double v : values) {
            array[i] = v;
            i++;
        }
        return array;
    }

    public static byte[] shortToBytes(short v) {
        return new byte[]{ (byte) (v & 0xff),
                           (byte) ((v >>> 8) & 0xff) };
    }

    public static byte[] intToBytes(int v) {
        return new byte[]{ (byte) (v & 0xff),
                           (byte) ((v >>> 8) & 0xff),
                           (byte) ((v >>> 16) & 0xff),
                           (byte) ((v >>> 24) & 0xff) };
    }

    public static byte[] longToBytes(long v) {
        return new byte[]{ (byte) (v & 0xff),
                           (byte) ((v >>> 8) & 0xff),
                           (byte) ((v >>> 16) & 0xff),
                           (byte) ((v >>> 24) & 0xff),
                           (byte) ((v >>> 32) & 0xff),
                           (byte) ((v >>> 40) & 0xff),
                           (byte) ((v >>> 48) & 0xff),
                           (byte) ((v >>> 56) & 0xff) };
    }

    public static byte[] floatToBytes(float v) {
        return intToBytes(Float.floatToIntBits(v));
    }

    public static byte[] doubleToBytes(double v) {
        return longToBytes(Double.doubleToLongBits(v));
    }

    public static short bytesToShort(byte[] array) {
        return bytesToShort(array, 0);
    }

    public static short bytesToShort(byte[] array, int offset) {
        return (short)
            ((array[offset] & 0xff) |
             ((array[offset+1] & 0xff) << 8));
    }

    public static int bytesToInt(byte[] array) {
        return bytesToInt(array, 0);
    }

    public static int bytesToInt(byte[] array, int offset) {
        return
            (array[0] & 0xff) |
            ((array[1] & 0xff) << 8) |
            ((array[2] & 0xff) << 16) |
            ((array[3] & 0xff) << 24);
    }

    public static long bytesToLong(byte[] array) {
        return bytesToLong(array, 0);
    }

    public static long bytesToLong(byte[] array, int offset) {
        return
            (array[offset] & 0xffL) |
            ((array[offset+1] & 0xffL) << 8) |
            ((array[offset+2] & 0xffL) << 16) |
            ((array[offset+3] & 0xffL) << 24) |
            ((array[offset+4] & 0xffL) << 32) |
            ((array[offset+5] & 0xffL) << 40) |
            ((array[offset+6] & 0xffL) << 48) |
            ((array[offset+7] & 0xffL) << 56);
    }

    public static float bytesToFloat(byte[] array) {
        return Float.intBitsToFloat(bytesToInt(array));
    }

    public static double bytesToDouble(byte[] array) {
        return Double.longBitsToDouble(bytesToLong(array));
    }
}
