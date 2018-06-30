package org.opendatakit.aggregate.constants.externalservice;

public enum NetvoteNetwork {
    ROPSTEN("ropsten"),
    PRIVATE("private");

    private String network;

    NetvoteNetwork(String value) {
        network = value;
    }

    public String getNetvoteNetworkValue() {
        return network;
    }
}
