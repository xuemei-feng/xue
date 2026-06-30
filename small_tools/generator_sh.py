import os
import netifaces
import xml.etree.ElementTree as ET

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)

cluster_number = 6
datanode_number_per_cluster = 8
datanode_port_start = 17600
cluster_id_start = 0
iftest = False

proxy_ip_list = [
    ["172.16.0.115", 50405],
    ["172.16.0.136", 50406],
    ["172.16.0.161", 50407],
    ["172.16.3.13", 50408],
    ["172.16.3.33", 50409],
    ["172.16.3.50", 50410],
]
coordinator_ip = "172.16.0.114"

proxy_num = len(proxy_ip_list)

IP_PREFIX = os.environ.get("UNILRC_IP_PREFIX", "172.16.2")
CLUSTER_XML = os.path.join(parent_path, "project", "config", "clusterInformation.xml")

# cluster_informtion = {cluster_id: {'proxy': 'ip:port', 'datanode': [[ip, port], ...]}, ...}
cluster_informtion = {}


def get_local_ip(interface_name):
    addresses = netifaces.ifaddresses(interface_name)
    return addresses[netifaces.AF_INET][0]['addr']


def get_interface_with_ip_prefix(prefix=IP_PREFIX):
    """Return a local cluster IP; prefer explicit LOCAL_IP from generate_run_proxy.sh."""
    env_ip = os.environ.get("LOCAL_IP", "").strip()
    if env_ip in ("%h", "%n", "%%"):
        env_ip = ""
    if env_ip:
        return env_ip

    interfaces = netifaces.interfaces()
    for interface in interfaces:
        try:
            addresses = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addresses:
                for addr in addresses[netifaces.AF_INET]:
                    ip = addr['addr']
                    if ip.startswith(prefix):
                        return ip
        except Exception as e:
            print("Error processing interface %s: %s" % (interface, e))
    return None


def _write_stop_commands(f, kill_proxy=True):
    f.write("pkill -9 run_datanode 2>/dev/null || true\n")
    if kill_proxy:
        f.write("pkill -9 run_proxy 2>/dev/null || true\n")


def load_clusters_from_xml(xml_path=CLUSTER_XML):
    clusters = {}
    tree = ET.parse(xml_path)
    root = tree.getroot()
    for cluster_el in root.findall("cluster"):
        cid = int(cluster_el.get("id"))
        proxy = cluster_el.get("proxy")
        if not proxy:
            raise ValueError("cluster id=%s missing proxy attribute" % cid)
        datanodes = []
        for dn in cluster_el.findall("./datanodes/datanode"):
            uri = dn.get("uri")
            if uri:
                datanodes.append(uri)
        clusters[cid] = {"proxy": proxy, "datanode": datanodes}
    return clusters


def sync_cluster_informtion_from_xml(clusters):
    """把 clusterInformation.xml 同步进 cluster_informtion 字典（与旧脚本结构兼容）。"""
    global cluster_informtion
    cluster_informtion = {}
    for cid in sorted(clusters):
        info = clusters[cid]
        datanode_list = []
        for uri in info["datanode"]:
            host, port = uri.rsplit(":", 1)
            datanode_list.append([host, int(port)])
        cluster_informtion[cid] = {
            "proxy": info["proxy"],
            "datanode": datanode_list,
        }


def local_roles(clusters, local_ip):
    """真实节点：本机 IP 对应哪些 proxy / datanode 角色。"""
    proxies = []
    datanodes = []
    for cid in sorted(clusters):
        info = clusters[cid]
        if info["proxy"].split(":", 1)[0] == local_ip:
            proxies.append((cid, info["proxy"]))
        for uri in info["datanode"]:
            if uri.split(":", 1)[0] == local_ip:
                datanodes.append((cid, uri))
    return proxies, datanodes


def generate_cluster_info_dict():
    """旧“端口模拟”：每个 proxy IP 上起 8 个同 IP 不同端口的 datanode。"""
    global cluster_informtion
    cluster_informtion = {}
    for i in range(proxy_num):
        new_cluster = {}
        new_cluster["proxy"] = proxy_ip_list[i][0] + ":" + str(proxy_ip_list[i][1])
        datanode_list = []
        for j in range(datanode_number_per_cluster):
            port = datanode_port_start + j
            if iftest:
                port = datanode_port_start + i * 100 + j
            datanode_list.append([proxy_ip_list[i][0], port])
        new_cluster["datanode"] = datanode_list
        cluster_informtion[i] = new_cluster


def generate_run_proxy_datanode_file():
    local_ip = get_interface_with_ip_prefix(prefix=IP_PREFIX)
    file_name = parent_path + '/run_proxy_datanode.sh'

    if not local_ip:
        print("Skip run_proxy_datanode.sh: no %s.* address on this host" % IP_PREFIX)
        with open(file_name, 'w') as f:
            _write_stop_commands(f)
        return

    clusters = load_clusters_from_xml()
    sync_cluster_informtion_from_xml(clusters)
    proxies, datanodes = local_roles(clusters, local_ip)

    # 防呆：网卡挂了整段 IP 别名时，未传 LOCAL_IP 且匹配多个角色则只写 stop
    if not os.environ.get("LOCAL_IP", "").strip() and (len(proxies) + len(datanodes)) > 1:
        print("Ambiguous local IP (%d roles matched); set LOCAL_IP explicitly. "
              "Wrote stop-only script." % (len(proxies) + len(datanodes)))
        with open(file_name, 'w') as f:
            _write_stop_commands(f)
        return

    with open(file_name, 'w') as f:
        _write_stop_commands(f)
        f.write("\n")

        for _cid, uri in datanodes:
            f.write("./project/cmake/build/run_datanode " + uri + " & \n")
        if datanodes:
            f.write("\n")

        if proxies:
            f.write("sleep 5s\n")
            f.write("\n")
            for _cid, proxy in proxies:
                f.write("./project/cmake/build/run_proxy " + proxy + "  & \n")
            f.write("\n")

    if not proxies and not datanodes:
        print("No cluster role for local_ip %s (client/coordinator?); wrote stop-only script" % local_ip)
        return

    desc = []
    if proxies:
        desc.append("proxy=" + ",".join("c%d:%s" % (cid, p) for cid, p in proxies))
    if datanodes:
        desc.append("datanode=" + ",".join(u for _cid, u in datanodes))
    print("local_ip %s -> %s" % (local_ip, "; ".join(desc)))


def generate_run_datanode_file():
    """供 start_datanode.sh 使用：只生成本机 datanode 启动脚本。"""
    local_ip = get_interface_with_ip_prefix(prefix=IP_PREFIX)
    file_name = parent_path + '/run_datanode.sh'

    with open(file_name, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("set -e\n\n")
        f.write("pkill -9 run_datanode || true\n\n")

        if not local_ip:
            f.write("echo \"No %s.* address on this host; skip datanode start.\"\n" % IP_PREFIX)
            f.write("exit 0\n")
            print("Skip run_datanode.sh: no %s.* address" % IP_PREFIX)
            return

        clusters = load_clusters_from_xml()
        _proxies, datanodes = local_roles(clusters, local_ip)

        if not os.environ.get("LOCAL_IP", "").strip() and len(datanodes) > 1:
            f.write("echo \"Ambiguous datanode roles for %s; set LOCAL_IP explicitly.\"\n" % local_ip)
            f.write("exit 1\n")
            print("Ambiguous datanode roles; wrote fail script for run_datanode.sh")
            return

        if not datanodes:
            f.write("echo \"No datanode role for local_ip=%s; skip.\"\n" % local_ip)
            f.write("exit 0\n")
            print("No datanode role for local_ip %s; wrote skip script" % local_ip)
            return

        for _cid, uri in datanodes:
            host, port = uri.rsplit(":", 1)
            bulk_port = int(port) + 50
            f.write("echo \"Starting datanode %s (bulk :%d)\"\n" % (uri, bulk_port))
            f.write("./project/cmake/build/run_datanode " + uri + " & \n")
        f.write("\n")

    print("run_datanode.sh: local_ip %s -> %s" % (
        local_ip, ",".join(u for _cid, u in datanodes)))


def generater_cluster_information_xml():
    file_name = parent_path + '/project/config/clusterInformation.xml'
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_informtion.keys():
        cluster = ET.SubElement(root, 'cluster', {
            'id': str(cluster_id),
            'proxy': cluster_informtion[cluster_id]["proxy"],
        })
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index, each_datanode in enumerate(cluster_informtion[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {
                'uri': str(each_datanode[0]) + ":" + str(each_datanode[1]),
            })
            if index == len(cluster_informtion[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_informtion) - 1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)


def cluster_generate_run_proxy_datanode_file(ip, port, i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) + '/cluster_run_proxy_datanode.sh'
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for each_datanode in cluster_informtion[0]["datanode"]:
            f.write("./project/cmake/build/run_datanode " + ip + ":" + str(each_datanode[1]) + " & \n")
        f.write("\n")
        f.write("./project/cmake/build/run_proxy " + ip + ":" + str(port) + " " + coordinator_ip + " & \n")
        f.write("\n")


if __name__ == "__main__":
    generate_cluster_info_dict()

    local_ip = get_interface_with_ip_prefix(prefix=IP_PREFIX)
    if local_ip:
        # 真实节点：所有 cluster 相关 IP（含 proxy/datanode）都生成本机脚本；
        # client(.1)/coordinator(.2) 会在 generate_* 里写成 stop-only / skip。
        generate_run_proxy_datanode_file()
        generate_run_datanode_file()
    else:
        generate_run_proxy_datanode_file()
        generate_run_datanode_file()

    # WARNING:
    # This writes project/config/clusterInformation.xml in legacy
    # "same proxy IP + multi-port datanodes" format.
    # Keep disabled by default to avoid accidentally overwriting
    # real-physical-node deployment configs.
    if os.environ.get("GEN_LEGACY_CLUSTER_XML", "0") == "1":
        generater_cluster_information_xml()
    else:
        print("Skip writing clusterInformation.xml (set GEN_LEGACY_CLUSTER_XML=1 to enable).")

    # cnt = 0
    # for proxy in proxy_ip_list:
    #     cluster_generate_run_proxy_datanode_file(proxy[0], proxy[1], cnt)
    #     cnt += 1
