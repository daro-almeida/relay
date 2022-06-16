import argparse
import os
import subprocess
import time
from collections import defaultdict as dd

host_extra_args = dd(list)
relay_to_id_range = dd(tuple)


def determine_relays_sleep_time(args):
    sleep = 7
    if args.no_gc_relays:
        sleep += 15
    return sleep


def determine_nodes_sleep_time(args):
    sleep = 3
    if args.no_gc_nodes:
        sleep += 15
    if args.sleep:
        sleep += args.sleep * args.nodes
    return sleep


def parse_line_to_host(line):
    parts = line.split(":")
    return parts[0], int(parts[1])


def hosts_from_file(filename, num):
    hosts = dd(list)
    with open(filename) as file:
        lines = file.readlines()
        n = 0
        for line in lines:
            host = parse_line_to_host(line)
            hosts[host[0]].append(host[1])
            n += 1
            if n >= num:
                break
    return hosts


def validate_args(args):
    pass


def build_start_relay_command(relay_host, i, args):
    command = ["python3", "start-relay.py", str(args.nodes), str(args.relays), str(i), args.list_nodes,
               args.list_relays, "-lf", args.log_folder, "-a", relay_host[0], "-p", str(relay_host[1])]
    if args.Xms_relays:
        command.extend(["-Xms", args.Xms_relays])
    if args.Xmx_relays:
        command.extend(["-Xmx", args.Xmx_relays])
    if args.no_gc_relays:
        command.append("-no_gc")
    if args.latency_matrix:
        command.extend(["-lm", args.latency_matrix])
    if args.bandwidth_config:
        command.extend(["-bc", args.bandwidth_config])
    if args.verbose:
        command.append("-v")
    command.append("\n")

    return command


def map_relay_to_id_range(args, relay_dict):
    relay_id = 0
    r = int(args.nodes % args.relays)
    for relay_address, _ in relay_dict.items():
        for relay_port in relay_dict[relay_address]:
            size = int(int(args.nodes / args.relays) + (1 if r > relay_id else 0))
            start = int(relay_id * int(args.nodes / args.relays) + max(0, min(r, relay_id)))
            end = int(start + size - 1)
            relay_to_id_range[(relay_address, relay_port)] = (start, end)
            relay_id += 1


def relay_from_map(i):
    for relay, rng in relay_to_id_range.items():
        if rng[0] <= i <= rng[1]:
            return relay
    print("error attributing relay to node")
    exit(1)  # error


def build_start_node_command(node_host, i, args):
    node_relay = relay_from_map(i)
    command = ["python3", "start-node.py", args.jar, node_relay[0], str(node_relay[1]), "-m",
               args.main_class, "-i", str(i), "-cf", args.config_file, "-lf", args.log_folder, "-a", node_host[0], "-p",
               str(node_host[1])]
    if args.Xms_nodes:
        command.extend(["-Xms", args.Xms_nodes])
    if args.Xmx_nodes:
        command.extend(["-Xmx", args.Xmx_nodes])
    if args.no_gc_nodes:
        command.append("-no_gc")
    if args.verbose:
        command.append("-v")
    if args.extra_args or args.extra_args_config:
        command.append("-e")
        if args.extra_args:
            command.extend(args.extra_args)
        if args.extra_args_config:
            command.extend(host_extra_args["%s:%s" % (node_host[0], node_host[1])])
    command.append("\n")
    if args.sleep:
        command.extend(["sleep", str(args.sleep), "\n"])

    return command


def run_processes(host_dict, build_command_func, args):
    i = 0
    for host_address, _ in host_dict.items():
        command = ["ssh" if args.ssh else "oarsh"]
        command.extend([host_address, "\n"])
        if args.cd:
            command.extend(["cd", args.cd, "\n"])

        for host_port in host_dict[host_address]:
            command.extend(build_command_func((host_address, host_port), i, args))
            i += 1
        if args.oar_job_id and not args.ssh:
            subprocess.Popen(command, env=dict(OAR_JOB_ID=str(args.oar_job_id), **os.environ))
        else:
            subprocess.Popen(command)


def get_extra_args_per_host(args):
    with open(args.extra_args_config, "r") as config:
        for line in config.readlines():
            parts = line.split(None, 1)
            host_extra_args[parts[0]] = parts[1].split(" ")


def start_experiment(relay_dict, node_dict, args):
    print("Starting up relays...")
    run_processes(relay_dict, build_start_relay_command, args)
    time.sleep(determine_relays_sleep_time(args))
    print("::::::RELAYS RUNNING::::::")

    map_relay_to_id_range(args, relay_dict)
    if args.extra_args_config:
        get_extra_args_per_host(args)

    print("Starting up nodes...")
    run_processes(node_dict, build_start_node_command, args)
    time.sleep(determine_nodes_sleep_time(args))
    print("::::::NODES RUNNING::::::")


def kill_processes(host_dict, args):
    for host_address, _ in host_dict.items():
        command = ["ssh" if args.ssh else "oarsh", host_address, "\n", "killall", "java", "\n"]
        if args.oar_job_id and not args.ssh:
            subprocess.run(command, env=dict(OAR_JOB_ID=str(args.oar_job_id), **os.environ))
        else:
            subprocess.run(command)


def end_experiment(relay_dict, node_dict, args):
    kill_processes(node_dict, args)
    kill_processes(relay_dict, args)


def main() -> int:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("jar")
    parser.add_argument("nodes", type=int, help="number of nodes")
    parser.add_argument("relays", type=int, nargs='?', default=1, help="number of relays")
    parser.add_argument("list_nodes", help="file with node list")
    parser.add_argument("list_relays", help="file with relay list")

    parser.add_argument("-ssh", action="store_true",
                        help="use ssh instead of oarsh, if ssh password environment in host is not defined, you'll be "
                             "asked to fill out the password on every remote access the script does")
    parser.add_argument("-cd",
                        help="after remote accessing one machine, change directory to a specified folder before "
                             "executing more commands")
    parser.add_argument("-oji", "--oar_job_id", type=int,
                        help="OAR_JOB_ID of experiment. If OAR_JOB_ID is already defined in environment, this is not "
                             "needed")
    parser.add_argument("-m", "--main_class", default="Main", help="Main class of .jar executable")
    parser.add_argument("-Xms_nodes")
    parser.add_argument("-Xmx_nodes")
    parser.add_argument("-Xms_relays")
    parser.add_argument("-Xmx_relays")
    parser.add_argument("-ngn", "--no_gc_nodes", action="store_true", help="disable garbage collector in nodes")
    parser.add_argument("-ngr", "--no_gc_relays", action="store_true", help="disable garbage collector in relays")
    parser.add_argument("-lf", "--log_folder", default="logs/", help="log folder")
    parser.add_argument("-cf", "--config_file", default="config.properties", help="config file for nodes")
    parser.add_argument("-lm", "--latency_matrix", help="file with latency matrix")
    parser.add_argument("-bc", "--bandwidth_config", help="file with bandwidth configuration")
    parser.add_argument("-e", "--extra_args", default=[], nargs="*", help="extra arguments for nodes")
    parser.add_argument("-ec", "--extra_args_config", help="file that specifies extra arguments for each node")
    parser.add_argument("-v", "--verbose", action="store_true", help="show process being launched for debugging")
    parser.add_argument("-t", "--time", type=int, help="run experiment during the given time, in seconds, or until "
                                                       "enter key is pressed")
    parser.add_argument("-s", "--sleep", type=float, help="sleep time in seconds between the initialization of each "
                                                          "node ")

    args = parser.parse_args()

    validate_args(args)

    relay_dict = hosts_from_file(args.list_relays, args.relays)
    node_dict = hosts_from_file(args.list_nodes, args.nodes)

    print("Removing any remnants of previous experiments if any...")
    end_experiment(relay_dict, node_dict, args)

    start_experiment(relay_dict, node_dict, args)

    time.sleep(1)

    if args.time:
        print("------------- Press enter to end experiment. Running for %ds --------------------" % args.time)
        subprocess.call('read -t %d' % args.time, shell=True, executable='/bin/bash')
    else:
        input("------------- Press enter to end experiment. --------------------")

    end_experiment(relay_dict, node_dict, args)

    time.sleep(1)
    print("Experiment ended successfully")

    return 0


if __name__ == '__main__':
    main()
