import subprocess
import getpass


def main() -> int:
    subprocess.run(['ssh', 'dicluster', 'export', 'OAR_JOB_ID=13944', '\n', 'oarsh', '172.30.10.116', 'hostname', '-I'])

    return 0


if __name__ == '__main__':
    main()
