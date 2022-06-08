import argparse
import os
import subprocess
import time
from collections import defaultdict as dd


def determine_relay_sleep_time(args):
    sleep = 7
    if args.no_gc_relays:
        sleep += 8
        if args.Xms_relays:
            sleep += args.Xms_relays / 5
    return sleep


def parse_line_to_host(line):
    parts = line.split(":")
    return parts[0], parts[1]


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
               args.list_relays, "-lf", args.log_folder, "-a", relay_host[0], "-p", relay_host[1]]
    if args.Xms_relays:
        command.extend(["-Xms", str(args.Xms_relays)])
    if args.Xmx_relays:
        command.extend(["-Xmx", str(args.Xmx_relays)])
    if args.no_gc_relays:
        command.append("-no_gc")
    if args.latency_matrix:
        command.extend(["-lm", args.latency_matrix])
    command.append("\n")

    return command


def map_relay_to_id_range(args, relay_dict):
    relay_to_id_range = dd(tuple)
    relay_id = 0
    r = args.nodes % args.relays
    for relay_address, _ in relay_dict.items():
        for relay_port in relay_dict[relay_address]:
            size = args.nodes / args.relays + (1 if r > relay_id else 0)
            start = relay_id * (args.nodes / args.relays) + max(0, min(r, relay_id))
            end = start + size - 1
            relay_to_id_range[(relay_address, relay_port)] = (start, end)
            relay_id += 1

    return relay_to_id_range


def relay_from_map(relay_to_id_range, i):
    for relay, rng in relay_to_id_range.items():
        if rng[0] <= i <= rng[1]:
            return relay
    exit(1)  # error


def build_start_node_command(node_host, i, args, relay_to_id_range):
    node_relay = relay_from_map(relay_to_id_range, i)
    command = ["python3", "start-node.py", args.jar, node_relay[0], node_relay[1], "-m",
               args.main_class, "-i", str(i), "-cf", args.config_file, "-lf", args.logfolder, "-a", node_host[0], "-p",
               node_host[1]]
    if args.Xms_nodes:
        command.extend(["-Xms", str(args.Xms_nodes)])
    if args.Xmx_nodes:
        command.extend(["-Xmx", str(args.Xmx_nodes)])
    if args.no_gc_nodes:
        command.append("-no_gc")
    command.append("-e")
    command.extend(args.extra_args)
    command.append("\n")

    return command


def run_processes(host_dict, build_command_func, args, relay_to_id_range):
    i = 0
    for host_address, _ in host_dict.items():
        command = ["ssh" if args.ssh else "oarsh"]
        command.extend([host_address, "\n"])
        if args.cd:
            command.extend(["cd", args.cd, "\n"])

        for host_port in host_dict[host_address]:
            command.extend(build_command_func((host_address, host_port), i, args, relay_to_id_range))
            i += 1

        if args.oar_job_id:
            subprocess.run(command, env=dict(OAR_JOB_ID=str(args.oar_job_id), **os.environ))
        else:
            subprocess.run(command)


def start_experiment(relay_dict, node_dict, args):
    relay_to_id_range = map_relay_to_id_range(args, relay_dict)

    run_processes(relay_dict, build_start_relay_command, args, [])
    time.sleep(determine_relay_sleep_time(args))
    run_processes(node_dict, build_start_node_command, args, relay_to_id_range)


def kill_processes(host_dict, args, jar):
    for host, _ in host_dict.items():
        command = "ssh" if args.ssh else "oarsh"
        command += " %s\n" % host[0]
        command += "kill $(ps aux | grep '%s' | awk '{print $2}')" % jar
        if args.oar_job_id:
            subprocess.run(command, shell=True, env=dict(OAR_JOB_ID=str(args.oar_job_id), **os.environ))
        else:
            subprocess.run(command, shell=True)


def end_experiment(relay_dict, node_dict, args):
    kill_processes(node_dict, args, args.jar)
    kill_processes(relay_dict, args, "relay.jar")
    print("Experiment ended successfully")


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
    parser.add_argument("-Xms_nodes", type=int)
    parser.add_argument("-Xmx_nodes", type=int)
    parser.add_argument("-Xms_relays", type=int)
    parser.add_argument("-Xmx_relays", type=int)
    parser.add_argument("-no_gc_nodes", action="store_true", help="disable garbage collector in nodes")
    parser.add_argument("-no_gc_relays", action="store_true", help="disable garbage collector in relays")
    parser.add_argument("-lf", "--log_folder", default="logs/", help="log folder")
    parser.add_argument("-cf", "--config_file", default="config.properties", help="config file for nodes")
    parser.add_argument("-lm", "--latency_matrix", help="file with latency matrix")
    parser.add_argument("-e", "--extra_args", default=[], nargs="*", help="extra arguments for nodes")

    args = parser.parse_args()

    validate_args(args)

    relay_dict = hosts_from_file(args.list_relays, args.relays)
    node_dict = hosts_from_file(args.list_nodes, args.nodes)

    start_experiment(relay_dict, node_dict, args)

    input("------------- Press enter to end experiment. --------------------")

    end_experiment(relay_dict, node_dict, args)

    return 0


if __name__ == '__main__':
    main()
