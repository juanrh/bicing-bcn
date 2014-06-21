package org.collprod.bicingbcn.ingestion.commons;

import java.io.Serializable;

/**
 * Generic version of org.apache.commons.lang.mutable.MutableObject.
 * 
 * Wraps an object into a mutable container, so the contents can be modified
 * in an inmutable context
 * */
public class MutableObject<V> implements Serializable {
	// autogenerated by Eclipse
	private static final long serialVersionUID = -9159052803256007319L;

	private V value;

	/**
	 * Null values are rejected, use MutableOptionalObject instead
	 * 
	 * @param value value to be wrapped. No call to clone() or any kind copy
	 * is performed (Cloneable is not even required)
	 * @throws NullPointerException in case value is null
	 * */
	public MutableObject(V value) { 
		if (value == null) {
			throw new NullPointerException("Null value");
		}
		this.value = value; 
	}

	public V get() {
		return value;
	}

	public void set(V value) {
		this.value = value;
	}

	@Override
	public String toString() {
		// mimicking implementation synthesized for an 
		// AutoValue (https://github.com/google/auto/tree/master/value)
		return "MutableObject{"
				+ "value=" + value
				+ "}";
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof MutableObject<?>) {
			MutableObject<?> that = (MutableObject<?>) o;
			// The call to equals() for wrapped values should
			// be checking the equality of the types of the parameters,
			// at least if equals() is properly implemented for V
			return (this.value.equals(that.value));
		}
		return false;
	}

	@Override
	public int hashCode() {
		// mimicking implementation synthesized for an 
		// AutoValue (https://github.com/google/auto/tree/master/value)
		int h = 1;
		h *= 1000003;
		h ^= value.hashCode();
		return h;
	}
}
