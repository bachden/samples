package nhb.zookeeper.leaderelection;

public interface LeaderUpdateListener {

	void updateLeaderInfo(byte[] leaderInfo);
}
