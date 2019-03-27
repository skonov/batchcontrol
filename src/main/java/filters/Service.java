package filters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Service {
	public static void main(String[] args) {
		List<Filter> filters = new ArrayList<>();
		Filter f1 = new Filter(true, "comp1", "1", "", null, "", null);
		Filter f2 = new Filter(true, "comp2", "2", "", null, "", null);
		Filter f3 = new Filter(true, "comp1", "3", "", null, "", null);
		Filter f4 = new Filter(false, "", "", "", null, "comp1", Arrays.asList("1","2"));
		Filter f5 = new Filter(false, "", "", "", null, "comp2", Arrays.asList("3","4"));
		filters.add(f4);
		filters.add(f5);
		filters.add(f1);
		filters.add(f2);
		filters.add(f3);

		
		
		new Service().groupFilters(filters);
	}
	
	void groupFilters(List<Filter> filters) {
		Map<String, Set<Filter>> compound = new HashMap<>();
		Map<String, Set<Filter>> simple = new HashMap<>();
		Map<String, Set<Filter>> combined = new HashMap<>();
		
		for (Filter f : filters) {
			if (f.compound) {
				addFilterToMap(compound, f, f.pField);
				if (simple.containsKey(f.pField)) {
					if (!combined.containsKey(f.pField)) {
						combined.put(f.pField, new HashSet<Filter>());
					}
					combined.get(f.pField).addAll(simple.get(f.pField));
					simple.remove(f.pField);
				}
			} else {
				addFilterToMap(simple, f, f.name);
				if (compound.containsKey(f.name)) {
					addFilterToMap(combined, f, f.name);
					simple.remove(f.name);
				}
			}
		}
		
		
		System.out.println("compound "+compound);
		System.out.println("combined "+combined);
		System.out.println("simple   "+simple);
		
	}
	
	void addFilterToMap(Map<String, Set<Filter>> map, Filter filter, String name) {
		if (!map.containsKey(name)) {
			map.put(name, new HashSet<Filter>());
		}
		map.get(name).add(filter);
	}

}

