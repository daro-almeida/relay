package relay.util;

import java.util.Objects;

public class SymPair<T extends Comparable<T>> {

	private final T first;
	private final T second;

	public SymPair(T first, T second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SymPair<?> pair = (SymPair<?>) o;
		return (first.equals(pair.first) && second.equals(pair.second)) ||
				(first.equals(pair.second) && second.equals(pair.first));
	}

	@Override
	public int hashCode() {
		return Objects.hash(first, second) + Objects.hash(second, first);
	}
}
