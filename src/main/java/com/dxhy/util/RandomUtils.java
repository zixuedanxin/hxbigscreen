package com.dxhy.util;

/**
 * Created by thinkpad on 2017/3/15.
 */
import java.util.Random;

public class RandomUtils
{
    private static final Random rand = new Random();
    private static final ThreadLocal<Random> rng = new ThreadLocal();
    public static final int FNV_offset_basis_32 = -2128831035;
    public static final int FNV_prime_32 = 16777619;
    public static final long FNV_offset_basis_64 = -3750763034362895579L;
    public static final long FNV_prime_64 = 1099511628211L;

    public static Random random()
    {
        Random ret = (Random)rng.get();
        if (ret == null) {
            ret = new Random(rand.nextLong());
            rng.set(ret);
        }
        return ret;
    }

    public static String ASCIIString(int length)
    {
        int interval = 95;

        byte[] buf = new byte[length];
        random().nextBytes(buf);
        for (int i = 0; i < length; i++) {
            if (buf[i] < 0)
                buf[i] = (byte)(-buf[i] % interval + 32);
            else {
                buf[i] = (byte)(buf[i] % interval + 32);
            }
        }
        return new String(buf);
    }

    public static long hash(long val)
    {
        return FNVhash64(val);
    }

    public static int FNVhash32(int val)
    {
        int hashval = -2128831035;

        for (int i = 0; i < 4; i++)
        {
            int octet = val & 0xFF;
            val >>= 8;

            hashval ^= octet;
            hashval *= 16777619;
        }

        return Math.abs(hashval);
    }

    public static long FNVhash64(long val)
    {
        long hashval = -3750763034362895579L;

        for (int i = 0; i < 8; i++)
        {
            long octet = val & 0xFF;
            val >>= 8;

            hashval ^= octet;
            hashval *= 1099511628211L;
        }

        return Math.abs(hashval);
    }
}
