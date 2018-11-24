package org.opendatakit.aggregate.constants.externalservice;

public enum NetvoteNetwork {
    ROPSTEN("netvote"),
    NETVOTE("ropsten");

    private String network;

    NetvoteNetwork(String value) {
        network = value;
    }

    public String getNetvoteNetworkValue() {
        return network;
    }
}
