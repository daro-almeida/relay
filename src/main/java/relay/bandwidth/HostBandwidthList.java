package relay.bandwidth;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.network.data.Host;
import relay.bandwidth.units.BitUnit;
import relay.bandwidth.units.ByteUnit;
import relay.util.HostPropertyList;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HostBandwidthList extends HostPropertyList<Pair<BandwidthBucket, BandwidthBucket>> {

	private static final Pattern PATTERN = Pattern.compile("(\\d+([.]\\d*)?|[.]\\d+)(.+)");

	public HostBandwidthList(List<Host> hostList, InputStream bandwidthConfig) throws IOException {
		super(hostList, bandwidthConfig);
	}

	private static BandwidthBucket parseBandwidth(Matcher match) {
		double bandwidth = Double.parseDouble(match.group(0));

		ThroughputType type = ThroughputType.valueOf(match.group(2));

		switch (type) {
			case BPS:
				return new BandwidthBucket(bandwidth, BitUnit.BIT);
			case KBPS:
				return new BandwidthBucket(bandwidth, BitUnit.KBIT);
			case MBPS:
				return new BandwidthBucket(bandwidth, BitUnit.MBIT);
			case GBPS:
				return new BandwidthBucket(bandwidth, BitUnit.GBIT);
			case PBPS:
				return new BandwidthBucket(bandwidth, BitUnit.PBIT);
			case B:
				return new BandwidthBucket(bandwidth, ByteUnit.BYTE);
			case KB:
				return new BandwidthBucket(bandwidth, ByteUnit.KB);
			case MB:
				return new BandwidthBucket(bandwidth, ByteUnit.MB);
			case GB:
				return new BandwidthBucket(bandwidth, ByteUnit.GB);
			case PB:
				return new BandwidthBucket(bandwidth, ByteUnit.PB);
			default:
				throw new IllegalStateException("Unexpected value: " + type);
		}
	}

	@Override
	protected Pair<BandwidthBucket, BandwidthBucket> parseString(String input) {
		String[] parts = input.split(" ");
		String inStr = parts[0];
		String outStr = parts[1];

		Matcher matcherIn = PATTERN.matcher(inStr);
		BandwidthBucket inBandwidth = parseBandwidth(matcherIn);

		Matcher matcherOut = PATTERN.matcher(outStr);
		BandwidthBucket outBandwidth = parseBandwidth(matcherOut);

		return new ImmutablePair<>(inBandwidth, outBandwidth);
	}

	public BandwidthBucket getInBandwidthBucket(Host host) {
		return propertyList.get(host).getLeft();
	}

	public BandwidthBucket getOutBandwidthBucket(Host host) {
		return propertyList.get(host).getRight();
	}

	private enum ThroughputType {
		BPS, KBPS, MBPS, GBPS, PBPS, //bits
		B, KB, MB, GB, PB             //bytes
	}
}
