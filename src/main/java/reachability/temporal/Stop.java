package reachability.temporal;

public class Stop {
	
	Object nodeId;
	int ts;
	int te;
	
	public Stop(Object isNodeId, int isTs, int isTe){
		nodeId = isNodeId;
		ts = isTs;
		te = isTe;
	}
	
	public boolean intersect(Stop other){
		if (!nodeId.equals(other.nodeId)) return false;
		if (ts > other.te) return false;
		if (te < other.ts) return false;
		return true;
	}

}
