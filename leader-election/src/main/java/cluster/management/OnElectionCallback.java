package cluster.management;

public interface OnElectionCallback {

    void onElectectedToBeLeader();

    void onWorker();

}
