package reachability.temporal;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RandomReach {
	public TitanGraph g;
	
	int numWalks, walkLength, budget;
	
	public RandomReach(int isNumWalks, int isWalkLength, int isBudget){
		g = TitanFactory.open("graph.properties");
		numWalks = isNumWalks;
		walkLength = isWalkLength;
		budget = isBudget;
		
		
	}
	
	boolean intersect(int s1, int e1, int s2, int e2){
		if (s1 <= s2 && s2 <= e1) return true;
		if (s1 <= e2 && e2 <= e1) return true;
		if (s2 <= s1 && s1 <= e2) return true;
		if (s2 <= e1 && e1 <= e2) return true;
		return false;
	}
	
	int max(int a, int b){
		return (a > b)? a : b;
	}
	
	int min(int a, int b){
		return (a < b)? a : b;
	}
	
	private class queueElement{
		Vertex node;
		int ts;
		int te;
		int numWalks;
		int walkLength;
		
		private queueElement(Vertex isNode, int isTs, int isTe, int isNumWalks, int isWalkLength){
			node = isNode;
			ts = isTs;
			te = isTe;
			numWalks = isNumWalks;
			walkLength = isWalkLength;
		}
	}
	
	public ArrayList<Stop> randomWalksP(int sourceNode, Object srcId, int ts, int te, boolean dir){
	
		ArrayList<Stop> stops = new ArrayList<Stop>();
		Queue<queueElement> q = new LinkedList<RandomReach.queueElement>();
		Vertex s = g.vertices(srcId).next();
		queueElement qe = new queueElement(s, ts, te, numWalks, walkLength);
		stops.add(new Stop(srcId, ts, te));
		
		q.add(qe);
		Random r = new Random();
		r.setSeed(System.currentTimeMillis());
		
		while (q.size() > 0){
			
			queueElement qe1 = (queueElement) q.remove();
			Vertex src = qe1.node;
			Iterator<Edge> elist = src.edges(Direction.OUT, dir? "fwdEdge" : "bwdEdge");
			ArrayList<queueElement> tempList = new ArrayList<queueElement>();
			
			while (elist.hasNext()){
				Edge e = elist.next();
				int ts1 = (int) e.value("start");
				int te1 = (int) e.value("end");
				int ta = max(qe1.ts,ts1), tb = min(qe1.te,te1);
				if (ta <= tb){
					Vertex v = e.inVertex();
					Object a = v.id();
					if (qe1.walkLength > 1)
						tempList.add(new queueElement(v, ta, tb, 0, qe1.walkLength - 1));
				}
			}
			int numEdges = tempList.size();
			if (numEdges == 0) continue;
			for (int i = 0; i < qe1.numWalks; i++){
				int edgeIndex = r.nextInt(numEdges);
				tempList.get(edgeIndex).numWalks += 1;
			}
			for (queueElement qa : tempList){
				if (qa.numWalks > 0){
					stops.add(new Stop(qa.node.id(),qa.ts,qa.te));
					q.add(qa);
				}
			}
			
		}
		
		
		return stops;
	}
	
	public int queryS(int source, Object srcId, int dest, Object dstId, int ts, int te){
		long ts1 = System.nanoTime();
		ArrayList<Stop> s = randomWalksP(source, srcId, ts, te, true);
		ArrayList<Stop> d = randomWalksP(dest, dstId, ts, te, false);
		long te1 = System.nanoTime();
		
		int nS = s.size(), dS = d.size();
		
		
		for (int i = 0; i < nS; i++){
			Stop x = s.get(i);
			for (int j = 0; j < dS; j++){
				Stop y = d.get(j);
				if (x.intersect(y)){
					
					return 1;
				}
			}
		}
		
		return 0;
	}
	
	public void processQueries(String queryFilename, String outputFilename){
		try{
			BufferedReader br = new BufferedReader(new FileReader(queryFilename));
			PrintWriter writer = new PrintWriter(outputFilename, "UTF-8");
			HashMap<String, Object> h = new HashMap<String, Object>();
			GraphTraversal<Vertex, Vertex> z = g.traversal().V();
			int nV = 0;
			while (z.hasNext()){
				Vertex v = z.next();
				h.put(v.property("name").value().toString(), v.id());
				nV += 1;
				if (nV % 100 == 0) System.out.println("Done " + nV + " vertices.");
			}
			
			String l;
			int numQ = 0;
			while ((l = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(l," ");
				st.nextToken();
				int u = Integer.parseInt(st.nextToken());
				int v = Integer.parseInt(st.nextToken());
				int ts = Integer.parseInt(st.nextToken());
				int te = Integer.parseInt(st.nextToken());
				
				Object uid = h.get("" + u);
				Object vid = h.get(""+v);
				long tstart = System.nanoTime();
				int a = queryS(u, uid, v, vid, ts, te);
				double elapsed = (System.nanoTime() - tstart)/Math.pow(10, 9);
				writer.println("Q: " + u + " " + v + " " + ts + " " + te + " " + a + " " + elapsed);
				System.out.println("Q: " + u + " " + v + " " + ts + " " + te + " " + a + " " + elapsed);
				numQ += 1;
				
				
			}
			br.close();
			writer.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void close(){
		g.close();
	}
	
	public static void main(String args[]){
		int nW = 1247, wL = 113, b = 2;
		Logger.getRootLogger().setLevel(Level.WARN);
		
		RandomReach rr = new RandomReach(nW, wL, b*nW*wL);
		try{
			rr.processQueries("/home/neha/ReachabilityTT/herbieCode/Datafiles/RealData/epinions/epinionsQuery", 
					"/home/neha/ReachabilityTT/herbieCode/Datafiles/RealData/epinions/epinionsQueryTitan");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		rr.close();
	}
}
