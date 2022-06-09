import argparse
import socket
import subprocess


def generate_command(args):
    command = ["java"]
    if args.Xms:
        command.append("-Xms%s" % args.Xms)
    if args.Xmx:
        command.append("-Xmx%s" % args.Xmx)
    if args.no_gc:
        command.extend(["-XX:+UnlockExperimentalVMOptions", "-XX:+UseEpsilonGC", "-XX:+AlwaysPreTouch"])
    command.extend(["-DlogFilename=%snode-%d" % (args.logfolder, args.id), "-cp", args.jar, args.main_class, "-conf",
                    args.config_file, "address=%s" % args.address, "port=%d" % args.port,
                    "relay_address=%s" % args.relay_address, "relay_port=%s" % args.relay_port])
    command.extend(args.extra_args)
    # command.append("&")
    return command


def validate_args(args):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("jar")
    parser.add_argument("relay_address")
    parser.add_argument("relay_port")

    parser.add_argument("-m", "--main_class", default="Main", help="Main class of .jar executable")
    parser.add_argument("-i", "--id", type=int, default=0, help="node ID")
    parser.add_argument("-cf", "--config_file", default="config.properties", help="config file")
    parser.add_argument("-Xms")
    parser.add_argument("-Xmx")
    parser.add_argument("-no_gc", action="store_true", help="disable garbage collector")
    parser.add_argument("-lf", "--logfolder", default="logs/", help="log folder")
    parser.add_argument("-a", "--address", default=socket.gethostbyname(socket.gethostname()),
                        help="local private address")
    parser.add_argument("-p", "--port", type=int, default=8581, help="node port")
    parser.add_argument("-e", "--extra_args", default=[], nargs="*")

    args = parser.parse_args()

    validate_args(args)

    command = generate_command(args)
    print("%s:" % args.address)
    print(*command)
    subprocess.Popen(command)

    return 0


if __name__ == '__main__':
    main()
