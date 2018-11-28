import org.apache.commons.lang.ArrayUtils;

import java.util.*;

/**
 * Created by andy.lai on 2017/2/7.
 */
public class RandomTest {

    public static void main(String[] args) {
        RandomTest test = new RandomTest();
        System.out.println(Arrays.toString(test.generateNumber(10)));
        int[] src = {1, 5, 8, 15, 20};
        System.out.println(Arrays.toString(test.insertAndSortNumber(src, 16)));
        System.out.println(Arrays.toString(test.insertAndSortNumber2(src, 16)));
    }

    private int[] generateNumber(int length) {
        int[] result = new int[length];
        Set<Integer> t = new HashSet<Integer>();
        for(int i=0; i<result.length; i++) {
            int n = (int)(Math.random()*100);
            if(t.contains(n)) {
               i--;
               continue;
            }
            t.add(n);
            result[i] = n;
        }
        return result;
    }

    private int[] insertAndSortNumber(int[] src, int number) {
        long s = System.nanoTime();
        int[] result = Arrays.copyOf(src, src.length+1);
        for(int i=src.length-1; i>=0; i--) {
            if(number <= result[i]) {
                result[i+1] = result[i];
            }else {
                result[i+1] = number;
                break;
            }
        }
        System.out.println(System.nanoTime()-s);
        return result;
    }

    private int[] insertAndSortNumber2(int[] src, int number) {
        long s = System.nanoTime();
        int[] result = Arrays.copyOf(src, src.length+1);
        result[src.length] = number;
//        Integer[] r = ArrayUtils.toObject(result);
//        Comparator<Integer> comp = Collections.reverseOrder();
        Arrays.sort(result);
        System.out.println(System.nanoTime()-s);
        return result;
    }
}
