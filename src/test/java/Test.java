import java.util.Random;

public class Test {

    public static void main(String[] args) {

        Random random = new Random();
        long count = 0L;
        int j = 100;
        for (int i = 0; i < j * 1000; i++) {
            count += 1;
        }

        System.out.println(count);
    }
}
