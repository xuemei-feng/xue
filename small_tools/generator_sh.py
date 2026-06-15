import os
import socket
import xml.etree.ElementTree as ET
import netifaces

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)
cluster_number = 6
datanode_number_per_cluster = 8
datanode_port_start = 17600
cluster_id_start = 0
iftest = False
RUN_ENV = os.environ.get("UNILRC_ENV", "half-sim").strip().lower()

proxy_ip_list = [
    ["10.10.1.3",50405],
    ["10.10.1.12",50405],
    ["10.10.1.21",50405],
    ["10.10.1.30",50405],
    ["10.10.1.39",50405],
    ["10.10.1.48",50405]
]
coordinator_ip = "10.10.1.2"

proxy_num = len(proxy_ip_list)

#cluster_informtion = {cluster_id:{'proxy':0.0.0.0:50005,'datanode':[[ip,port],...]},}

def get_local_ip(interface_name):
    addresses = netifaces.ifaddresses(interface_name)
    return addresses[netifaces.AF_INET][0]['addr']

def get_interface_with_ip_prefix(prefix="10.10.1"):
    """返回所有以 prefix 开头的本机 IPv4 地址（包括主地址和别名）。"""
    ips = []
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        try:
            addresses = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addresses:
                for addr in addresses[netifaces.AF_INET]:
                    ip = addr['addr']
                    if ip.startswith(prefix):
                        ips.append(ip)
        except Exception as e:
            print(f"Error processing interface {interface}: {e}")
    if ips:
        return ips
    return None

def get_primary_ip_with_prefix(prefix="10.10.1"):
    """返回第一个匹配的前缀 IP（向后兼容）。"""
    result = get_interface_with_ip_prefix(prefix)
    if isinstance(result, list) and len(result) > 0:
        return result[0]
    return None


cluster_informtion = {}


def get_proxy_port_base():
    ports = []
    for info in cluster_informtion.values():
        proxy = info.get("proxy", "")
        if ":" not in proxy:
            continue
        try:
            ports.append(int(proxy.rsplit(":", 1)[1]))
        except ValueError:
            continue
    if ports:
        return min(ports)
    return proxy_ip_list[0][1]


def load_cluster_information_xml(xml_path):
    """从 project/config/clusterInformation.xml 填充 cluster_informtion，与手工编辑的配置一致。"""
    global cluster_informtion
    cluster_informtion = {}
    tree = ET.parse(xml_path)
    root = tree.getroot()
    for cluster_el in root.findall("cluster"):
        cid = int(cluster_el.get("id"))
        proxy = cluster_el.get("proxy")
        if not proxy:
            raise ValueError("cluster id=%s missing proxy attribute" % cid)
        datanode_list = []
        for dn in cluster_el.findall("./datanodes/datanode"):
            uri = dn.get("uri")
            if not uri:
                continue
            host, port_s = uri.rsplit(":", 1)
            datanode_list.append([host, int(port_s)])
        cluster_informtion[cid] = {"proxy": proxy, "datanode": datanode_list}


def resolve_cluster_id_for_local_ip(local_ips):
    """本机 IP 与某 cluster 的 proxy 或任一 datanode 主机一致时，返回所有匹配的 cluster id 列表。"""
    if not local_ips:
        return []
    if isinstance(local_ips, str):
        local_ips = [local_ips]
    matched = []
    for local_ip in local_ips:
        for cid, info in cluster_informtion.items():
            proxy_host = info["proxy"].split(":", 1)[0]
            if proxy_host == local_ip:
                if cid not in matched:
                    matched.append(cid)
            for host, _port in info["datanode"]:
                if host == local_ip:
                    if cid not in matched:
                        matched.append(cid)
    return matched


def generate_cluster_info_dict():
    for i in range(proxy_num):
        new_cluster = {}
        
        new_cluster["proxy"] = proxy_ip_list[i][0]+":"+str(proxy_ip_list[i][1])
        datanode_list = []
        for j in range(datanode_number_per_cluster):
            port = datanode_port_start + j
            if iftest:
                port = datanode_port_start + i*100 + j
            datanode_list.append([proxy_ip_list[i][0], port])
        new_cluster["datanode"] = datanode_list
        cluster_informtion[i] = new_cluster


def convert_cluster_info_to_local():
    """将当前 cluster_informtion 转换为纯本地部署：127.0.0.1 + 全局唯一端口。"""
    global cluster_informtion
    local_cluster_information = {}
    datanode_port = datanode_port_start
    proxy_port_base = get_proxy_port_base()

    for cluster_id in sorted(cluster_informtion.keys()):
        local_cluster = {}
        local_cluster["proxy"] = "127.0.0.1:" + str(proxy_port_base + cluster_id)
        datanode_list = []
        for _ in cluster_informtion[cluster_id]["datanode"]:
            datanode_list.append(["127.0.0.1", datanode_port])
            datanode_port += 1
        local_cluster["datanode"] = datanode_list
        local_cluster_information[cluster_id] = local_cluster
    cluster_informtion = local_cluster_information
            
def generate_run_proxy_datanode_file():
    if RUN_ENV == "local":
        file_name = parent_path + '/run_proxy_datanode.sh'
        with open(file_name, 'w') as f:
            f.write("pkill -9 run_datanode\n")
            f.write("pkill -9 run_proxy\n")
            f.write("\n")

            for cluster_id in sorted(cluster_informtion.keys()):
                for each_datanode in cluster_informtion[cluster_id]["datanode"]:
                    f.write("./project/cmake/build/run_datanode " + str(each_datanode[0]) + ":" + str(each_datanode[1]) + " & \n")
            f.write("\n")
            f.write("sleep 5s\n")
            f.write("\n")
            for cluster_id in sorted(cluster_informtion.keys()):
                f.write("./project/cmake/build/run_proxy " + str(cluster_informtion[cluster_id]["proxy"]) + " " + " & \n")
            f.write("\n")
        return

    local_ips = get_interface_with_ip_prefix(prefix="10.10.1")
    if local_ips is None or (isinstance(local_ips, list) and len(local_ips) == 0):
        print("Warning: no 10.10.1.x address found; skip run_proxy_datanode.sh")
        return
    if isinstance(local_ips, str):
        local_ips = [local_ips]

    cluster_ids = resolve_cluster_id_for_local_ip(local_ips)
    if not cluster_ids:
        # No match found, but we have 10.10.1.x IPs — single-machine multi-cluster setup:
        # generate script for ALL clusters in clusterInformation.xml
        if len(cluster_informtion) > 0:
            print("Single-machine multi-cluster mode: generating run_proxy_datanode.sh for all",
                  len(cluster_informtion), "clusters (local_ips:", local_ips[0], "...)")
            cluster_ids = sorted(cluster_informtion.keys())
        else:
            print("Skip run_proxy_datanode.sh: no cluster in clusterInformation.xml matches local_ips",
                  local_ips)
            return

    file_name = parent_path + '/run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        print("Generating for cluster_ids:", cluster_ids)
        for cid in sorted(cluster_ids):
            for each_datanode in cluster_informtion[cid]["datanode"]:
                f.write("./project/cmake/build/run_datanode " + str(each_datanode[0]) + ":" + str(each_datanode[1]) + " & \n")
        f.write("\n")
        f.write("sleep 5s\n")
        f.write("\n")
        for cid in sorted(cluster_ids):
            f.write("./project/cmake/build/run_proxy " + str(cluster_informtion[cid]["proxy"]) + " " + " & \n")
        f.write("\n")
        
def generater_cluster_information_xml():
    file_name = parent_path + '/project/config/clusterInformation.xml'
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_informtion.keys():
        cluster = ET.SubElement(root, 'cluster', {'id': str(cluster_id), 'proxy': cluster_informtion[cluster_id]["proxy"]})
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index,each_datanode in enumerate(cluster_informtion[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {'uri': str(each_datanode[0])+":"+str(each_datanode[1])})
            #datanode.text = '\n\t\t\t'
            if index == len(cluster_informtion[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_informtion)-1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    #root.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)
            
def cluster_generate_run_proxy_datanode_file(ip, port, i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for each_datanode in cluster_informtion[0]["datanode"]:
            f.write("./project/cmake/build/run_datanode "+ip+":"+str(each_datanode[1])+" & \n")
        f.write("\n") 
        f.write("./project/cmake/build/run_proxy "+ip+":"+str(port)+" "+coordinator_ip+" & \n")   
        f.write("\n")

if __name__ == "__main__":
    xml_path = os.path.join(parent_path, "project", "config", "clusterInformation.xml")
    if os.path.isfile(xml_path):
        load_cluster_information_xml(xml_path)
    else:
        print("Warning:", xml_path, "not found; falling back to generate_cluster_info_dict()")
        generate_cluster_info_dict()
    if RUN_ENV == "local":
        convert_cluster_info_to_local()
    # print(cluster_informtion)
    generate_run_proxy_datanode_file()
    #generate_run_proxy_datanode_file() # for test
    # 不再默认重写 clusterInformation.xml：update_all -> generate_run_proxy 会在各节点执行本脚本，
    # 若总是调用 generater_cluster_information_xml() 会覆盖人工编辑的配置。
    # 需要按脚本内 proxy_ip_list 重新生成 XML 时，在 small_tools 目录执行：
    #   GENERATE_CLUSTER_INFORMATION_XML=1 python3 generator_sh.py
    if os.environ.get("GENERATE_CLUSTER_INFORMATION_XML") == "1":
        generater_cluster_information_xml()
    
    # cnt = 0
    # for proxy in proxy_ip_list:
        #cluster_generate_run_proxy_datanode_file(proxy[0], proxy[1], cnt)
        #cnt += 1
    
