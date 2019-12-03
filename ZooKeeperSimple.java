package com.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperSimple {

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// 创建一个与服务器的连接
		ZooKeeper zk = new ZooKeeper(ZookeeperConstant.URL, 1000 * 1, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});

		// 创建一个目录节点
		zk.create("/stu17034460218", "stu17034460218".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		// 创建一个子目录节点
		zk.create("/stu17034460218/testChildPathOne", "testChildDataOne".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData("/stu17034460218", false, null)));

		// 取出子目录节点列表
		System.out.println(zk.getChildren("/stu17034460218", true));

		// 修改子目录节点数据
		zk.setData("/stu17034460218/testChildPathOne", "modifyChildDataOne".getBytes(), -1);
		System.out.println("目录节点状态：[" + zk.exists("/stu17034460218", true) + "]");

		// 创建另外一个子目录节点
		zk.create("/stu17034460218/testChildPathTwo", "testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(new String(zk.getData("/stu17034460218/testChildPathTwo", true, null)));

		// 删除子目录节点
		zk.delete("/stu17034460218/testChildPathTwo", -1);
		zk.delete("/stu17034460218/testChildPathOne", -1);

		// 删除父目录节点
		zk.delete("/stu17034460218", -1);
		System.out.println("成功删除节点!");
		// 关闭连接
		zk.close();
	}

}
