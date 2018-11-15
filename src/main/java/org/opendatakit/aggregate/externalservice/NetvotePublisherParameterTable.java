package org.opendatakit.aggregate.externalservice;


import org.opendatakit.aggregate.constants.externalservice.NetvoteConsts;
import org.opendatakit.aggregate.constants.externalservice.NetvoteNetwork;
import org.opendatakit.common.persistence.CommonFieldsBase;
import org.opendatakit.common.persistence.DataField;
import org.opendatakit.common.persistence.Datastore;
import org.opendatakit.common.persistence.exception.ODKDatastoreException;
import org.opendatakit.common.security.User;
import org.opendatakit.common.web.CallingContext;

import java.util.HashMap;
import java.util.Map;

public final class NetvotePublisherParameterTable extends CommonFieldsBase {

    private static final String TABLE_NAME = "_netvote_publisher";

    private static final DataField FORM_ID_PROPERTY = new DataField("FORM_ID",
            DataField.DataType.STRING, true, 4096L);

    private static final DataField ACCESS_KEY_PROPERTY = new DataField("ACCESS_KEY",
            DataField.DataType.STRING, true, 4096L);

    private static final DataField SECRET_KEY_PROPERTY = new DataField("SECRET_KEY",
            DataField.DataType.STRING, true, 4096L);

    private static final DataField NETWORK_PROPERTY = new DataField("NETWORK",
            DataField.DataType.STRING, true, 4096L);

    private static final DataField OWNER_EMAIL_PROPERTY = new DataField(
            "OWNER_EMAIL", DataField.DataType.STRING, true, 4096L);

    private static final Map<String, String> networkToLambda = new HashMap<String,String>();


    private static NetvotePublisherParameterTable relation = null;


    NetvotePublisherParameterTable(String schemaName) {
        super(schemaName, TABLE_NAME);
        fieldList.add(FORM_ID_PROPERTY);
        fieldList.add(ACCESS_KEY_PROPERTY);
        fieldList.add(SECRET_KEY_PROPERTY);
        fieldList.add(NETWORK_PROPERTY);
        fieldList.add(OWNER_EMAIL_PROPERTY);

    }

    /**
     * Construct an empty entity. Only called via {@link #getEmptyRow(User)}
     *
     * @param ref
     * @param user
     */
    private NetvotePublisherParameterTable(NetvotePublisherParameterTable ref, User user) {
        super(ref, user);
    }

    @Override
    public CommonFieldsBase getEmptyRow(User user) {
        return new NetvotePublisherParameterTable(this, user);
    }

    public String getOwnerEmail() {
        return getStringField(OWNER_EMAIL_PROPERTY);
    }

    public void setOwnerEmail(String value) {
        if (!setStringField(OWNER_EMAIL_PROPERTY, value)) {
            throw new IllegalArgumentException("overflow ownerEmail");
        }
    }

    public void setFormId(String value) {
        if (!setStringField(FORM_ID_PROPERTY, value)) {
            throw new IllegalArgumentException("overflow form Id");
        }
    }

    public void setAccessKeyProperty(String value) {
        if (!setStringField(ACCESS_KEY_PROPERTY, value)) {
            throw new IllegalArgumentException("overflow accessKey");
        }
    }

    public void setSecretKeyProperty(String value) {
        if (!setStringField(SECRET_KEY_PROPERTY, value)) {
            throw new IllegalArgumentException("overflow secretKey");
        }
    }

    public void setNetworkProperty(String value) {
        //confirm is valid enum
        NetvoteNetwork.valueOf(value.toUpperCase());
        if (!setStringField(NETWORK_PROPERTY, value)) {
            throw new IllegalArgumentException("overflow network");
        }
    }


    public String getFormIdProperty() {
        return getStringField(FORM_ID_PROPERTY);
    }

    public String getAccessKeyProperty() {
        return getStringField(ACCESS_KEY_PROPERTY);
    }

    public String getSecretKeyProperty() {
        return getStringField(SECRET_KEY_PROPERTY);
    }

    public String getNetworkProperty() {
        return getStringField(NETWORK_PROPERTY);
    }

    public static synchronized final NetvotePublisherParameterTable assertRelation(
            CallingContext cc) throws ODKDatastoreException {
        if (relation == null) {
            NetvotePublisherParameterTable relationPrototype;
            Datastore ds = cc.getDatastore();
            User user = cc.getCurrentUser();
            relationPrototype = new NetvotePublisherParameterTable(ds.getDefaultSchemaName());
            ds.assertRelation(relationPrototype, user); // may throw exception...
            // at this point, the prototype has become fully populated
            relation = relationPrototype; // set static variable only upon success...
        }
        return relation;
    }
}
