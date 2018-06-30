/*
 * Copyright (C) 2010 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.aggregate.constants.externalservice;

import java.util.HashMap;
import java.util.Map;

import org.opendatakit.aggregate.datamodel.FormElementModel.ElementType;

/**
 *
 * @author wbrunette@gmail.com
 * @author mitchellsundt@gmail.com
 *
 */
public class NetvoteConsts {

    public static final String PRIVATE_ADD_OBSERVATION = "private-add-observation";
    public static final String ROPSTEN_ADD_OBSERVATION = "netvote-add-observation";
    public static final String IPFS_GATEWAY = "https://ipfs.infura.io";

    public static final Map<NetvoteNetwork, String>  networkLambdas = new HashMap<NetvoteNetwork, String>();
    static {
        networkLambdas.put(NetvoteNetwork.PRIVATE, PRIVATE_ADD_OBSERVATION);
        networkLambdas.put(NetvoteNetwork.ROPSTEN, ROPSTEN_ADD_OBSERVATION);
    }
}
