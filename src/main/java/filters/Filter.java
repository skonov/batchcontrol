package filters;

import java.util.List;

public class Filter {
	boolean compound;
	String pField;
	String pFieldValue;
	String sField;
	List<String> sFieldValues;
	String name;
	List<String> values;
	
	public Filter(boolean compound, String pField, String pFieldValue, String sField, List<String> sFieldValues,
			String name, List<String> values) {
		this.compound = compound;
		this.pField = pField;
		this.pFieldValue = pFieldValue;
		this.sField = sField;
		this.sFieldValues = sFieldValues;
		this.name = name;
		this.values = values;
	}

	@Override
	public String toString() {
		return "compound="+compound+" "+pField+":"+pFieldValue+" "+name+":"+values;
	}
}

