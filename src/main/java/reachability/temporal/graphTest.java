package reachability.temporal;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer; 
import java.util.HashMap;


public class graphTest {
	public void createGraph(String graphFilename){
		TitanGraph g = TitanFactory.open("graph.properties");
		HashMap<Integer, Vertex> customIdMap = new HashMap<Integer, Vertex>();
		
		BufferedReader br = null;
		FileReader fr = null;
		try {
			br = new BufferedReader(new FileReader(graphFilename));
			String l;

			while ((l = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(l," ");
				int u = Integer.parseInt(st.nextToken());
				int v = Integer.parseInt(st.nextToken());
				int ts = Integer.parseInt(st.nextToken());
				int te = Integer.parseInt(st.nextToken());
				
				if (!customIdMap.containsKey(u)){
					Vertex unode = g.addVertex(T.label,"vertex", "name", ""+u);
					customIdMap.put(u, unode);
				}
				if (!customIdMap.containsKey(v)){
					Vertex vnode = g.addVertex(T.label,"vertex", "name", ""+v);
					customIdMap.put(v, vnode);
				}
				TitanTransaction tx = g.newTransaction();
				Vertex unode = customIdMap.get(u), vnode = customIdMap.get(v);
				unode.addEdge("fwdEdge", vnode, "start", ts, "end", te);
				vnode.addEdge("bwdEdge", vnode, "start", ts, "end", te);
				tx.commit();
				tx.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null) br.close();
				if (fr != null) fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		/*
		Vertex v1 = g.addVertex(T.label, "vertex", "name", "v1", "start",1,"end",10);
		Vertex v2 = g.addVertex(T.label, "vertex", "name", "v2", "start", 3, "end", 12);
		v1.addEdge("propertyEdge", v2, "start", 4, "end", 9);*/
		g.close();
		
	}
	
	public void queryGraph(){
		TitanGraph g = TitanFactory.open("graph.properties");
		System.out.println("NumEdges = " + g.traversal().E().count().next());
		g.close();
	}
	
	
	public static void main(String args[]){
		graphTest g = new graphTest();
		//g.queryGraph();
		g.createGraph("/home/neha/ReachabilityTT/herbieCode/Datafiles/RealData/epinions/epinionsEdgeList");
		
	}
}
