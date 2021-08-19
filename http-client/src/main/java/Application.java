import java.util.Arrays;
import java.util.List;

public class Application {
    private static String WORKER_ADDRESS1 = "http://localhost:8081/task";
    private static String WORKER_ADDRESS2 = "http://localhost:8082/task";

    public static void main(String[] args) {
        Aggregator aggregator = new Aggregator();
        String task1 = "10,200";
        String task2 = "2323,123245,832026213123";

        List<String> results = aggregator.sendTaskToWorkers(Arrays.asList(WORKER_ADDRESS1, WORKER_ADDRESS2), Arrays.asList(task1, task2));
        for(String result : results){
            System.out.println(result);
        }
    }


}
