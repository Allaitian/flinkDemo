package com;

import py4j.GatewayServer;
import java.util.LinkedList;
import java.util.List;
public class StackEntryPoint {

    private Stack stack;

    public StackEntryPoint() {
        stack = new Stack();
        stack.push("Initial Item");
    }

    public Stack getStack() {
        return stack;
    }

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new StackEntryPoint());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }

}



class Stack {
    private List<String> internalList = new LinkedList<String>();

    public void push(String element) {
        internalList.add(0, element);
    }

    public String pop() {
        return internalList.remove(0);
    }

    public List<String> getInternalList() {
        return internalList;
    }

    public void pushAll(List<String> elements) {
        for (String element : elements) {
            this.push(element);
        }
    }
}
