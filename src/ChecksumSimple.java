import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChecksumSimple {

static boolean finished = false;
static long totalChecksumBytes = 0;
    public static void main (String[] args) {
        String path = args[0];
        MessageDigest md = null;
        long fileSize = 0;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] buffer = new byte[128 * 1024];


        File file =new File(path);
        File[] files;
        if(file.isDirectory()) {
            files = file.listFiles();
        } else {
            files = new File[] {file};
        }
        System.out.println("Will transfer " + files.length+ " files");

        new MonitorThread().start();
        long init = System.currentTimeMillis();

        long read;
        long remaining = fileSize;
        for (int i = 0; i <files.length ; i++) {
            InputStream is = null;
            try {
                File currentFile = files[i];
                is = new FileInputStream(currentFile);

            } catch (Exception e) {
                e.printStackTrace();
            }
            long checksumStartTime = System.currentTimeMillis();
            DigestInputStream dis = new DigestInputStream(is, md);
            try {
                while ((read = dis.read(buffer, 0, buffer.length)) > 0) {
                    totalChecksumBytes += read;
                }
                dis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finished = true;

            byte[] digest = md.digest();
            String hex = (new HexBinaryAdapter()).marshal(digest);
            System.out.println("File:" + files[i] + " Checksum  " + hex +
                    " duration" + (System.currentTimeMillis()- checksumStartTime) +
                    " ms time:" + (System.currentTimeMillis() - init) / 1000.0 + " s");

        }
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }


    public static class MonitorThread extends Thread {
        long lastChecksumBytes = 0;

        @Override
        public void run() {
            try {
                while (!finished) {
                    double checksumThrInMbps = 8 * (totalChecksumBytes-lastChecksumBytes)/(1024*1024);
                    System.out.println("Checksum thr:" + checksumThrInMbps + " Mb/s read" +
                            humanReadableByteCount(totalChecksumBytes, false));
                    lastChecksumBytes = totalChecksumBytes;
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}


