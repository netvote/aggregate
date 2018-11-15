package org.opendatakit.aggregate.externalservice;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.opendatakit.aggregate.constants.common.ExternalServicePublicationOption;
import org.opendatakit.aggregate.constants.common.ExternalServiceType;
import org.opendatakit.aggregate.constants.common.OperationalStatus;
import org.opendatakit.aggregate.constants.externalservice.NetvoteConsts;
import org.opendatakit.aggregate.exception.ODKExternalServiceCredentialsException;
import org.opendatakit.aggregate.exception.ODKExternalServiceException;
import org.opendatakit.aggregate.form.IForm;
import org.opendatakit.aggregate.format.element.BasicElementFormatter;
import org.opendatakit.aggregate.format.element.OhmageJsonElementFormatter;
import org.opendatakit.aggregate.format.header.BasicHeaderFormatter;
import org.opendatakit.aggregate.submission.Submission;
import org.opendatakit.common.persistence.CommonFieldsBase;
import org.opendatakit.common.persistence.exception.ODKDatastoreException;
import org.opendatakit.common.persistence.exception.ODKEntityPersistException;
import org.opendatakit.common.persistence.exception.ODKOverQuotaException;
import org.opendatakit.common.security.common.EmailParser;
import org.opendatakit.common.utils.WebUtils;
import org.opendatakit.common.web.CallingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NetvotePublisher extends AbstractExternalService implements ExternalService {

    private static final Gson gson;
    private static final Map<String, Boolean> cache = new ConcurrentHashMap<String, Boolean>();

    static {
        GsonBuilder builder = new GsonBuilder()
        .setLenient()
        .setPrettyPrinting();
        gson = builder.create();

    }


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

    public NetvotePublisher(IForm form, String formId, String accessKey, String secretKey, String network,
                      ExternalServicePublicationOption externalServiceOption, String ownerEmail, CallingContext cc)
            throws ODKDatastoreException {
        this(newEntity(NetvotePublisherParameterTable.assertRelation(cc), cc), form, externalServiceOption,
                ownerEmail, cc);

        objectEntity.setFormId(formId);
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


    private String uploadItemToIPFS(String apiKey, String entryName, ByteArrayBody item,  CallingContext cc) throws IOException {
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.STRICT)
                .setCharset(UTF_CHARSET);

        Map<String,String> headers = new HashMap<>();
        headers.put("x-api-key", apiKey);

        builder.addPart(entryName, item);

        HttpResponse response = super.sendHttpRequest(POST, NetvoteConsts.IPFS_PIN_URL, builder.build(), null, headers, cc);
        return WebUtils.readResponse(response);
    }


    private List<String> uploadToIPFS(List<OhmageJsonTypes.Survey> surveys, Map<UUID, ByteArrayBody> photos,
                              CallingContext cc) throws ClientProtocolException, IOException, ODKExternalServiceException {

        String apiKey = objectEntity.getAccessKeyProperty();

        List<String> results = new ArrayList<>();

        // emit the file streams for all the media attachments
        for (Map.Entry<UUID, ByteArrayBody> entry : photos.entrySet()) {
            String hash = uploadItemToIPFS(apiKey, entry.getKey().toString(), entry.getValue(), cc);
            results.add(hash);
        }

        return results;
    }

    private static synchronized void unlock(String id) {
        cache.remove(id);
    }

    private static synchronized boolean tryLock(String id) {
        if(cache.containsKey(id)){
            return false;
        }
        cache.put(id, true);
        return true;
    }

    private class Payload {
        String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    private class SubmissionObj {
        String odkFormId;
        String odkSubmitId;
        String submission;
        List<String> attachments;

        public String getSubmission() {
            return submission;
        }

        public void setSubmission(String submission) {
            this.submission = submission;
        }

        public List<String> getAttachments() {
            return attachments;
        }

        public void setAttachments(List<String> attachments) {
            this.attachments = attachments;
        }

        public String getOdkFormId() {
            return odkFormId;
        }

        public void setOdkFormId(String odkFormId) {
            this.odkFormId = odkFormId;
        }

        public String getOdkSubmitId() {
            return odkSubmitId;
        }

        public void setOdkSubmitId(String odkSubmitId) {
            this.odkSubmitId = odkSubmitId;
        }
    }

    private String getJwtToken(CallingContext cc) throws IOException {
        String submitKey = objectEntity.getSecretKeyProperty();
        String apiKey = objectEntity.getAccessKeyProperty();
        String formId = objectEntity.getFormIdProperty();

        Map<String,String> headers = new HashMap<>();
        headers.put("x-api-key", apiKey);
        headers.put("Authorization", "Bearer "+submitKey);

        String url = String.format("%s/form/%s/auth/jwt", NetvoteConsts.NETROSA_ENDPOINT, formId);
        HttpEntity entity = new StringEntity("blank");

        HttpResponse response = super.sendHttpRequest(POST, url, entity, null, headers, cc);
        String respJson = WebUtils.readResponse(response);

        //TODO: remove this log line
        logger.info("NETVOTE token JSON: "+respJson);

        Type type = new TypeToken<Map<String, String>>(){}.getType();
        Map<String, String> myMap = gson.fromJson(respJson, type);
        return myMap.get("token");
    }

    private void submitToNetrosa(SubmissionObj obj, CallingContext cc) throws IOException {
        String apiKey = objectEntity.getAccessKeyProperty();
        String formId = objectEntity.getFormIdProperty();

        String submissionString = gson.toJson(obj);
        Payload p = new Payload();
        p.setValue(submissionString);

        String payloadBody = gson.toJson(p);
        logger.info("NETVOTE: Sending payload body="+payloadBody);

        String jwtToken = getJwtToken(cc);

        HttpEntity entity = new StringEntity(payloadBody);

        Map<String,String> headers = new HashMap<>();
        headers.put("x-api-key", apiKey);
        headers.put("Authorization", "Bearer "+jwtToken);

        String url = String.format("%s/form/%s/submission", NetvoteConsts.NETROSA_ENDPOINT, formId);

        HttpResponse response = super.sendHttpRequest(POST, url, entity, null, headers, cc);
        String resp = WebUtils.readResponse(response);
        logger.info("NETVOTE: Submitted payload to API"+resp+", url="+url);
    }

    @Override
    protected void insertData(Submission submission, CallingContext cc) throws ODKExternalServiceException {
        try {
            boolean lock = tryLock(submission.getKey().getKey());

            if(!lock){
                logger.info("NETVOTE: duplication execution of "+submission.getKey().getKey()+", bailing");
                return;
            }

            logger.info("NETVOTE: Starting Publish of data: "+submission);

            OhmageJsonTypes.Survey survey = new OhmageJsonTypes.Survey();
            OhmageJsonElementFormatter formatter = new OhmageJsonElementFormatter();

            // called purely for side effects
            submission.getFormattedValuesAsRow(null, formatter, false, cc);
            survey.setResponses(formatter.getResponses());

            List<String> attachments = uploadToIPFS(Collections.singletonList(survey), formatter.getPhotos(), cc);
            String submissionJson = gson.toJson(survey);

            SubmissionObj p = new SubmissionObj();
            p.setAttachments(attachments);
            p.setSubmission(submissionJson);
            p.setOdkFormId(submission.getFormId());
            p.setOdkSubmitId(submission.getKey().getKey());

            submitToNetrosa(p, cc);

        } catch (ODKExternalServiceCredentialsException e) {
            fsc.setOperationalStatus(OperationalStatus.BAD_CREDENTIALS);
            try {
                persist(cc);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new ODKExternalServiceException("unable to persist bad credentials state", ex);
            }
            throw e;
        } catch (ODKExternalServiceException e) {
            throw e;// don't wrap these
        } catch (Exception e) {
            throw new ODKExternalServiceException(e);
        } finally {
            unlock(submission.getKey().getKey());
        }
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
