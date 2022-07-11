import argparse
import socket
import subprocess


def generate_command(args):
    command = ["java"]
    if args.Xms:
        command.append("-Xms%s" % args.Xms)
    if args.Xmx:
        command.append("-Xmx%s" % args.Xmx)
    if args.max_buffer_size:
        command.append("-XX:MaxDirectMemorySize=%s" % args.max_buffer_size)
    if args.no_gc:
        command.extend(["-XX:+UnlockExperimentalVMOptions", "-XX:+UseEpsilonGC", "-XX:+AlwaysPreTouch"])
    command.extend(
        ["-DlogFilename=%srelay-%d" % (args.logfolder, args.relay_id), "-cp", "relay.jar", "StartRelay",
         str(args.nodes), str(args.relays), str(args.relay_id), args.list_nodes, args.list_relays, "-a", args.address,
         "-p", str(args.port)])
    if args.latency_matrix:
        command.extend(["-lm", args.latency_matrix])
    if args.bandwidth_config:
        command.extend(["-bc", args.bandwidth_config])
    if args.sleep:
        command.extend(["-s", args.sleep])
    # command.append("&")
    return command


def validate_args(args):
    if not args.nodes or args.nodes < 1:
        print("please indicate a number of nodes of at least one")


def main() -> int:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("nodes", type=int, help="number of nodes")
    parser.add_argument("relays", type=int, nargs='?', default=1, help="number of relays")
    parser.add_argument("relay_id", type=int, nargs='?', default=0, help="relay ID")
    parser.add_argument("list_nodes", help="file with node list")
    parser.add_argument("list_relays", help="file with relay list")

    parser.add_argument("-Xms", help="min memory heap size")
    parser.add_argument("-Xmx", help="max memory heap size")
    parser.add_argument("-mbs", "--max_buffer_size", help="max buffer size")
    parser.add_argument("-no_gc", action="store_true", help="disable garbage collector")
    parser.add_argument("-lf", "--logfolder", default="logs/", help="log folder")
    parser.add_argument("-a", "--address", default=socket.gethostbyname(socket.gethostname()),
                        help="local private address")
    parser.add_argument("-p", "--port", type=int, default=9082, help="relay port")
    parser.add_argument("-lm", "--latency_matrix", help="file with latency matrix")
    parser.add_argument("-bc", "--bandwidth_config", help="file with bandwidth configuration")
    parser.add_argument("-s", "--sleep", help="sleep time in ms before connecting to other relays")
    parser.add_argument("-v", "--verbose", action="store_true", help="show process being launched for debugging")

    args = parser.parse_args()

    validate_args(args)

    command = generate_command(args)
    if args.verbose:
        print("%s:" % args.address)
        print(*command)
    subprocess.Popen(command)

    return 0


if __name__ == '__main__':
    main()
