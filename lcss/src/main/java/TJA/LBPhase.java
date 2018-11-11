package TJA;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lcss.util.CompareUtil;

public class LBPhase {
	private int k;
	private Map<Integer,Double> map;
	public LBPhase(int k, Map<Integer, Double> map) {
		super();
		this.k = k;
		this.map = map;
	}
	public Map top_k(){
		int a[]=new int[this.k];
		Map<Integer, Double> resultmap=new HashMap<Integer, Double>();
		Map<Integer, Double> TopKmap=new HashMap<Integer, Double>();
		resultmap=CompareUtil.sortMapByValueDescending(this.map);
		for (Entry<Integer, Double> entry : resultmap.entrySet())
			  TopKmap.put(entry.getKey(), entry.getValue());
		return TopKmap;
	}
}
