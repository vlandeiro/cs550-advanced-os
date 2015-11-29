import boto.ec2
import sys

def get_running_instances(access_id, secret_key):
    conn = boto.ec2.connect_to_region("us-west-2",
                                      aws_access_key_id=access_id,
                                      aws_secret_access_key=secret_key)
    reservations = conn.get_all_reservations()
    instances = []
    for reservation in reservations:
        for instance in reservation.instances:
            if instance.state == 'running':
                instances.append(instance.ip_address)
    return conn, instances

def print_usage(args):
    print("Usage: python %s credentials.csv" % args[0])
    sys.exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) != 2:
        print_usage(args)
    with open(args[1]) as credentials_fd:
        credentials_fd.readline()
        username, access_id, secret_key = credentials_fd.readline().strip().split(",")
    conn, instances = get_running_instances(access_id, secret_key)
    count = 0
    username = 'ec2-user'
    for node_count in [1,2,4,8,16]:
        list_hosts = " ".join([username + "@" + ip for ip in instances[:node_count]])
        print("clus%d %s" % (node_count, list_hosts))
    sys.stderr.write("Copy the output to /etc/clusters to work with clusterssh.\n")
    # for j, inst in enumerate(instances):
    #     print("\t%s\tnode%d" % (inst, j))
    conn.close()
