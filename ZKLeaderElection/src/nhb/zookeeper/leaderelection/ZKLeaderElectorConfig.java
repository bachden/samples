package nhb.zookeeper.leaderelection;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ZKLeaderElectorConfig {

	@Builder.Default
	private int sessionTimeout = 30000; // ms

	@Builder.Default
	private int connectionTimeout = Integer.MAX_VALUE; // ms

	@Builder.Default
	private String servers = "localhost:2181"; // default localhost
	
	@Builder.Default
	private long sessionId = -1;
	
	@Builder.Default
	private byte[] sessionPassword = null;
	
	@Builder.Default
	private boolean canBeReadOnly = false;

	@Builder.Default
	private String electionPath = "/leader_election";

	@Builder.Default
	private String nodePrefix = "p_";
}
