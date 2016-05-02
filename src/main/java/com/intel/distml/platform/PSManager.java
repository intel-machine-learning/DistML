package com.intel.distml.platform;

import akka.actor.ActorRef;
import com.intel.distml.util.Logger;

import java.util.LinkedList;

/**
 * Created by yunlong on 4/28/16.
 */
public class PSManager {

    static class PSNode {
        String addr;
        String executorId;
        ActorRef actor;

        PSNode(String addr, String executorId, ActorRef actor) {
            this.addr = addr;
            this.executorId = executorId;
            this.actor = actor;
        }

        public String toString() {
            return "[ps " + addr + "]";
        }
    }

    static interface PSMonitor {
        void switchServer(int index, String addr, ActorRef actor);
        void psFail();
    }

    LinkedList<PSNode>[] servers;
    PSMonitor monitor;

    public PSManager(PSMonitor monitor, int psCount) {
        this.monitor = monitor;
        this.servers = new LinkedList[psCount];
        for (int i = 0; i < servers.length; i++) {
            servers[i] = new LinkedList<PSNode>();
        }
    }

    public LinkedList<ActorRef> getAllActors() {
        LinkedList<ActorRef> actors = new LinkedList<ActorRef>();
        for (int i = 0; i < servers.length; i++) {
            if (servers[i] != null) {
                for (PSNode node : servers[i]) {
                    if (node != null) {
                        actors.add(node.actor);
                    }
                }
            }
        }
        return actors;
    }

    public ActorRef[] getActors() {
        ActorRef[] actors = new ActorRef[servers.length];
        for (int i = 0; i < servers.length; i++) {
            if (servers[i] != null) {
                PSNode node = servers[i].getFirst();
                if (node != null) {
                    actors[i] = node.actor;
                }
            }
        }
        return actors;
    }

    public String[] getAddrs() {
        String[] addrs = new String[servers.length];
        for (int i = 0; i < servers.length; i++) {
            if (servers[i] != null) {
                PSNode node = servers[i].getFirst();
                if (node != null) {
                    addrs[i] = node.addr;
                }
            }
        }
        return addrs;
    }

    public ActorRef getActor(int index) {
        if (servers[index] != null) {
            PSNode node = servers[index].getFirst();
            if (node != null) {
                 return node.actor;
            }
        }
        return null;
    }

    public String getAddr(int index) {
        if (servers[index] != null) {
            PSNode node = servers[index].getFirst();
            if (node != null) {
                return node.addr;
            }
        }
        return null;
    }

    public void serverTerminated(ActorRef actor) {
        PSNode node = null;
        int index = -1;
        for (int i = 0; i < servers.length; i++) {
            for (PSNode n : servers[i]) {
                if (n.actor.equals(actor)) {
                    node = n;
                    index = i;
                    break;
                }
            }
            if (node != null) break;
        }

        serverTerminated(index, node);
    }

    public void serverTerminated(String executorId) {
        PSNode node = null;
        int index = -1;
        for (int i = 0; i < servers.length; i++) {
            for (PSNode n : servers[i]) {
                if (n.executorId.equals(executorId)) {
                    node = n;
                    index = i;
                    break;
                }
            }
            if (node != null) break;
        }

        serverTerminated(index, node);
    }

    public void serverTerminated(int index, PSNode node) {
        log("parameter server terminated: " + index + ", " + node);
        boolean isPrimary = (servers[index].getFirst() == node);

        servers[index].remove(node);
        if (servers[index].size() == 0) {
            monitor.psFail();
        }
        else if (isPrimary) {
            node = servers[index].getFirst();
            monitor.switchServer(index, node.addr, node.actor);
        }
    }


    /**
     * add new parameter server to the list.
     *
     *
     * @param index
     * @param addr
     * @param ref
     * @return whether the server be primary
     */
    public boolean serverAvailable(int index, String executorId, String addr, ActorRef ref) {
        servers[index].add(new PSNode(addr, executorId, ref));
        return (servers[index].size() == 1);
    }
/*
    private PSNode getNode(int executorId) {
        for (int i = 0; i < servers.length; i++) {
            for (PSNode node : servers[i]) {
                if (node.executorId == executorId) {
                    return node;
                }
            }
        }

        return null;
    }

    private PSNode getNode(ActorRef a) {
        for (int i = 0; i < servers.length; i++) {
            for (PSNode node : servers[i]) {
                if (node.actor.equals(a)) {
                    return node;
                }
            }
        }

        return null;
    }

    private int getIndex(ActorRef a) {
        for (int i = 0; i < servers.length; i++) {
            for (PSNode node : servers[i]) {
                if (node.actor.equals(a)) {
                    return i;
                }
            }
        }

        return -1;
    }

    private int getIndex(int executorId) {
        for (int i = 0; i < servers.length; i++) {
            for (PSNode node : servers[i]) {
                if (node.executorId == executorId) {
                    return i;
                }
            }
        }

        return -1;
    }

    private int getIndexAsPrimary(ActorRef a) {
        for (int i = 0; i < servers.length; i++) {
            if (servers[i].size() == 0) continue;

            PSNode node = servers[i].getFirst();
            if (node.actor.equals(a)) {
                return i;
            }
        }

        return -1;
    }

    private int getIndexAsPrimary(int executorId) {
        for (int i = 0; i < servers.length; i++) {
            if (servers[i].size() == 0) continue;

            PSNode node = servers[i].getFirst();
            if (node.executorId == executorId) {
                return i;
            }
        }

        return -1;
    }
*/

    private void log(String msg) {
        Logger.info(msg, "PSManager");
    }
    private void warn(String msg) {
        Logger.warn(msg, "PSManager");
    }
}
