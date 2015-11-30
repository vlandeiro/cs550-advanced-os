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
    # create file for clusterssh
    print("Copy the output to /etc/clusters to work with clusterssh:")
    list_hosts = " ".join([username + "@" + ip for ip in instances])
    print("aws %s" % (list_hosts))
    print("Cassandra seeds:")
    print('"%s"' % ",".join(instances[:2]))
    print("Redis create command:")
    cmd_base = "redis-trib.rb create --replicas 0 %s"
    list_hosts = " ".join([ip + ":7000" for ip in instances])
    print(cmd_base % list_hosts)
    # for j, inst in enumerate(instances):
    #     print("\t%s\tnode%d" % (inst, j))
    conn.close()
