import subprocess
import argparse
import socket


def generate_command(args):
    command = ["java"]
    if args.Xms:
        command.append("-Xms%dg" % args.Xms)
    if args.Xmx:
        command.append("-Xmx%dg" % args.Xmx)
    if args.no_gc:
        command.extend(["-XX:+UnlockExperimentalVMOptions", "-XX:+UseEpsilonGC"])
    command.extend(
        ["-DlogFilename=%srelay-%d" % (args.logfolder, args.relay_id), "-cp", "relay.jar", "relay.Relay", args.address,
         str(args.port), str(args.nodes), str(args.relays), str(args.relay_id), args.list_nodes, args.list_relays])
    if args.latency_matrix:
        command.append(args.latency_matrix)
    command.append("&")
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

    parser.add_argument("-Xms", type=int)
    parser.add_argument("-Xmx", type=int)
    parser.add_argument("-no_gc", action="store_true", help="disable garbage collector")
    parser.add_argument("-lf", "--logfolder", default="logs/", help="log folder")
    parser.add_argument("-a", "--address", default=socket.gethostbyname(socket.gethostname()),
                        help="local private address")
    parser.add_argument("-p", "--port", type=int, default=9082, help="relay port")
    parser.add_argument("-lm", "--latency_matrix", help="file with latency matrix")

    args = parser.parse_args()

    validate_args(args)

    command = generate_command(args)
    print("{} on {}:{}".format(*command, args.address, args.port))
    subprocess.Popen(command)

    return 0


if __name__ == '__main__':
    main()
