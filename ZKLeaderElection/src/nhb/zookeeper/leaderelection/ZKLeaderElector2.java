package nhb.zookeeper.leaderelection;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ZKLeaderElector2 {

	private final Object electionSyncMonitorObj = new Object();

	private ZooKeeper zooKeeper;

	private final byte[] myInfo;
	private final ZKLeaderElectorConfig config;

	@Getter
	private byte[] currentLeaderInfo;

	@Getter
	private String electionPath;

	@Getter
	private final String nodePrefix;

	@Getter
	private long myPathId;

	@Getter
	private String myPath;

	@Getter
	private String followedNodePath = null;

	private final Set<LeaderUpdateListener> listeners = new CopyOnWriteArraySet<>();

	private Watcher watcher = new Watcher() {

		@Override
		public void process(WatchedEvent event) {
			ZKLeaderElector2.this.process(event);
		}
	};

	public ZKLeaderElector2(ZKLeaderElectorConfig config, byte[] myInfo) {
		this.config = config;
		this.myInfo = myInfo;
		this.nodePrefix = config.getNodePrefix();
	}

	public void start() {
		if (this.zooKeeper == null) {
			synchronized (electionSyncMonitorObj) {
				if (this.zooKeeper == null) {
					try {
						if (config.getSessionId() >= 0) {
							this.zooKeeper = new ZooKeeper(this.config.getServers(), this.config.getSessionTimeout(),
									this.watcher, config.getSessionId(), config.getSessionPassword(),
									config.isCanBeReadOnly());
						} else {
							this.zooKeeper = new ZooKeeper(this.config.getServers(), this.config.getSessionTimeout(),
									this.watcher, config.isCanBeReadOnly());
						}
					} catch (Exception e) {
						throw new RuntimeException("Error while creating zooKeeper instance", e);
					}

					try {
						final Stat nodeStat = zooKeeper.exists(this.getElectionPath(), true);
						if (nodeStat == null) {
							this.electionPath = this.zooKeeper.create(this.getElectionPath(), null, Ids.OPEN_ACL_UNSAFE,
									CreateMode.PERSISTENT);
						}

						if (this.myInfo != null) {
							this.join();
						} else {
							updateLeaderInfo();
						}
					} catch (KeeperException | InterruptedException e) {
						throw new RuntimeException("Error while creating election node: " + this.getElectionPath(), e);
					}

				}
			}
		}
	}

	private long extractPathId(String path) {
		return Long.valueOf(path.substring(this.getAbsoluteChildNodePrefix().length()));
	}

	private String getAbsoluteChildNodePrefix() {
		return this.electionPath + "/" + this.nodePrefix;
	}

	private void join() throws KeeperException, InterruptedException {
		if (this.myInfo == null) {
			throw new NullPointerException("Cannot join cluster with null info");
		}
		this.myPath = this.zooKeeper.create(this.getAbsoluteChildNodePrefix(), this.myInfo, Ids.READ_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		this.myPathId = this.extractPathId(this.myPath);

		this.attemptForLeaderPosition();
	}

	private void attemptForLeaderPosition() throws KeeperException, InterruptedException {
		synchronized (this.electionSyncMonitorObj) {
			List<String> children = this.zooKeeper.getChildren(this.electionPath, false);
			long ltAndClosestId = Long.MIN_VALUE;
			String tobeFollowedCandidate = null;
			for (String child : children) {
				String absChildPath = this.electionPath + "/" + child;
				long pathId = this.extractPathId(absChildPath);
				if (pathId > ltAndClosestId && pathId < this.myPathId) {
					ltAndClosestId = pathId;
					tobeFollowedCandidate = absChildPath;
				}

				if (tobeFollowedCandidate == null) {
					// I'm new leader
					// override info on the election path
					this.zooKeeper.setData(this.electionPath, this.myInfo, -1);
				} else if (!tobeFollowedCandidate.equals(this.followedNodePath)) {
					this.follow(tobeFollowedCandidate);
				}
			}
		}
	}

	private void follow(String tobeFollowed) throws KeeperException, InterruptedException {
		if (this.followedNodePath != null) {
			this.zooKeeper.exists(this.followedNodePath, false);
			this.followedNodePath = null;
		}

		if (tobeFollowed != null) {
			this.followedNodePath = tobeFollowed;
			this.zooKeeper.exists(this.followedNodePath, true);
		}
	}

	private void process(WatchedEvent event) {
		try {
			final EventType eventType = event.getType();
			if (eventType != null) {
				String path = event.getPath();
				if (path != null) {
					if (EventType.NodeDeleted.equals(eventType)) {
						if (path.equalsIgnoreCase(this.followedNodePath)) {
							try {
								attemptForLeaderPosition();
							} catch (KeeperException | InterruptedException e) {
								log.error("Error while processing watched event", e);
							}
						}
					} else if (EventType.NodeDataChanged.equals(eventType)) {
						if (path.equals(this.electionPath)) {
							this.updateLeaderInfo();
						}
					}
				}
			}

			if (event.getState() == KeeperState.Disconnected) {
				synchronized (electionSyncMonitorObj) {
					this.zooKeeper = null;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Error while processing watched event", e);
		}
	}

	private void updateLeaderInfo() throws KeeperException, InterruptedException {
		byte[] leaderInfo = this.zooKeeper.getData(this.electionPath, false, null);
		this.currentLeaderInfo = leaderInfo;
		this.dispatchUpdate();
	}

	private void dispatchUpdate() {
		for (LeaderUpdateListener listener : this.listeners) {
			try {
				listener.updateLeaderInfo(currentLeaderInfo);
			} catch (Exception e) {
				log.error("Error while dispatch update event (still continue signal to other listener)", e);
			}
		}
	}

	public void addListener(LeaderUpdateListener listener) {
		if (listener != null) {
			this.listeners.add(listener);
		} else {
			throw new NullPointerException("Listener cannot be null");
		}
	}

	public boolean removeListener(LeaderUpdateListener listener) {
		if (listener != null) {
			return this.listeners.remove(listener);
		}
		return false;
	}
}
