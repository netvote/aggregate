package org.opendatakit.aggregate.externalservice;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.entity.ContentType;
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

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
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

    private AWSLambda getLambdaClient() {
        Regions region = Regions.fromName("us-east-1");

        BasicAWSCredentials credentials = new
                BasicAWSCredentials(objectEntity.getAccessKeyProperty(), objectEntity.getSecretKeyProperty());

        return AWSLambdaClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();
    }

    public void submitToBlockchain(AWSLambda client, String scope, String submitId, String reference, long timestamp) throws ODKExternalServiceException {
        String payload = String.format("{\"scope\":\"%s\",\"submitId\":\"%s\",\"reference\":\"%s\",\"timestamp\":%d}", scope, submitId, reference, timestamp);

        InvokeRequest req = new InvokeRequest()
                .withFunctionName(objectEntity.getLambdaName())
                .withInvocationType(InvocationType.Event)
                .withPayload(payload);

        InvokeResult ir = client.invoke(req);
        if(ir.getStatusCode() != 202){
            throw new ODKExternalServiceException("failure syncing to blockchain");
        }
        logger.info(String.format("NETVOTE: Published to blockchain, payload: %s", payload));
    }

    public List<String> uploadToIPFS(List<OhmageJsonTypes.Survey> surveys, Map<UUID, ByteArrayBody> photos,
                              CallingContext cc) throws ClientProtocolException, IOException, ODKExternalServiceException,
            URISyntaxException {


        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.STRICT)
                .setCharset(UTF_CHARSET);

        ContentType utf8Text = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), UTF_CHARSET);
        // emit the configured publisher parameters if the values are non-empty...

        // emit the client identity and the json representation of the survey...
        builder.addTextBody("survey", gson.toJson(surveys), utf8Text);

        // emit the file streams for all the media attachments
        for (Map.Entry<UUID, ByteArrayBody> entry : photos.entrySet()) {
            builder.addPart(entry.getKey().toString(), entry.getValue());
        }

        // ADD TO IPFS
        HttpResponse response = super.sendHttpRequest(POST, NetvoteConsts.IPFS_PIN_URL, builder.build(), null, cc);
        String responseString = WebUtils.readResponse(response);
        logger.info("NETVOTE: IPFS upload response: "+responseString);

        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode == HttpServletResponse.SC_UNAUTHORIZED) {
            throw new ODKExternalServiceCredentialsException("failure from server: " + statusCode
                    + " response: " + responseString);
        } else if (statusCode >= 300) {
            throw new ODKExternalServiceException("failure from server: " + statusCode + " response: "
                    + responseString);
        }

        String[] res = responseString.split("\\{");
        List<String> results = new ArrayList<>();

        for(String s: res){
            if(!s.trim().isEmpty()) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> ipfsResultMap = gson.fromJson(responseString, type);
                results.add(ipfsResultMap.get("Name"));
            }
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

            List<String> references = uploadToIPFS(Collections.singletonList(survey), formatter.getPhotos(), cc);

            if(references.size() > 0) {
                AWSLambda client = getLambdaClient();
                for (String ref : references) {
                    logger.info("NETVOTE: IPFS ref = " + ref);
                    long timestamp = submission.getSubmissionDate().getTime();
                    submitToBlockchain(client, submission.getFormId(), submission.getKey().getKey(), ref, timestamp);
                }
            }

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
