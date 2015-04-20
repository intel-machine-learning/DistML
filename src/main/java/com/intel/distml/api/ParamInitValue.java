package com.intel.distml.api;

public class ParamInitValue<T> {
	
	public enum Builtin {
		Zero,			// initialize with 0, 0.0f or 0.0lf 
		Random, 		// initialize with random value between value1 and value2,
		Specified		// initialize with specified value1
	}
	
	T value1, value2;

}
