package org.opendatakit.aggregate.externalservice;

import org.opendatakit.aggregate.constants.common.ExternalServicePublicationOption;
import org.opendatakit.aggregate.constants.common.ExternalServiceType;
import org.opendatakit.aggregate.constants.common.OperationalStatus;
import org.opendatakit.aggregate.exception.ODKExternalServiceException;
import org.opendatakit.aggregate.form.IForm;
import org.opendatakit.aggregate.format.element.BasicElementFormatter;
import org.opendatakit.aggregate.format.header.BasicHeaderFormatter;
import org.opendatakit.aggregate.submission.Submission;
import org.opendatakit.common.persistence.CommonFieldsBase;
import org.opendatakit.common.persistence.exception.ODKDatastoreException;
import org.opendatakit.common.persistence.exception.ODKEntityPersistException;
import org.opendatakit.common.persistence.exception.ODKOverQuotaException;
import org.opendatakit.common.security.common.EmailParser;
import org.opendatakit.common.web.CallingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NetvotePublisher extends AbstractExternalService implements ExternalService {

    /**
     * Datastore entity specific to this type of external service
     */
    private final NetvotePublisherParameterTable objectEntity;
    private Logger logger =LoggerFactory.getLogger(NetvotePublisher.class);

    private NetvotePublisher(NetvotePublisherParameterTable entity, FormServiceCursor formServiceCursor,
                       IForm form, CallingContext cc) {
        super(form, formServiceCursor, new BasicElementFormatter(true, true, true, false),
                new BasicHeaderFormatter(true, true, true), cc);
        objectEntity = entity;
    }

    private NetvotePublisher(NetvotePublisherParameterTable entity, IForm form,
                       ExternalServicePublicationOption externalServiceOption, String ownerEmail, CallingContext cc)
            throws ODKDatastoreException {
        this(entity, createFormServiceCursor(form, entity, externalServiceOption,
                ExternalServiceType.NETVOTE_PUBLISHER, cc), form, cc);
        objectEntity.setOwnerEmail(ownerEmail);
    }

    public NetvotePublisher(FormServiceCursor formServiceCursor, IForm form, CallingContext cc)
            throws ODKDatastoreException {
        this(retrieveEntity(NetvotePublisherParameterTable.assertRelation(cc), formServiceCursor, cc),
                formServiceCursor, form, cc);
    }

    public NetvotePublisher(IForm form, String accessKey, String secretKey, String network,
                      ExternalServicePublicationOption externalServiceOption, String ownerEmail, CallingContext cc)
            throws ODKDatastoreException {
        this(newEntity(NetvotePublisherParameterTable.assertRelation(cc), cc), form, externalServiceOption,
                ownerEmail, cc);

        objectEntity.setAccessKeyProperty(accessKey);
        objectEntity.setSecretKeyProperty(secretKey);
        objectEntity.setNetworkProperty(network);
        persist(cc);
    }


    @Override
    protected String getOwnership() {
        return objectEntity.getOwnerEmail().substring(EmailParser.K_MAILTO.length());
    }

    @Override
    protected CommonFieldsBase retrieveObjectEntity() {
        return objectEntity;
    }

    @Override
    protected List<? extends CommonFieldsBase> retrieveRepeatElementEntities() {
        return null;
    }

    @Override
    protected void insertData(Submission submission, CallingContext cc) throws ODKExternalServiceException {
        logger.info("NETVOTE: insert data!: "+submission);
    }

    @Override
    public void initiate(CallingContext cc) throws ODKExternalServiceException, ODKEntityPersistException, ODKOverQuotaException, ODKDatastoreException {
        logger.info("NETVOTE: initiate");
        fsc.setIsExternalServicePrepared(true);
        fsc.setOperationalStatus(OperationalStatus.ACTIVE);
        persist(cc);

        // upload data to external service
        postUploadTask(cc);
    }

    @Override
    public String getDescriptiveTargetString() {
        return "NETVOTE://"+objectEntity.getNetworkProperty();
    }
}
