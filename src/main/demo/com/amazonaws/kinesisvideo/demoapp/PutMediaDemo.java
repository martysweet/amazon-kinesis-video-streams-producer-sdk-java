package com.amazonaws.kinesisvideo.demoapp;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.kinesisvideo.demoapp.auth.AuthHelper;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoAsyncClient;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoPutMedia;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoPutMediaClient;
import com.amazonaws.services.kinesisvideo.PutMediaAckResponseHandler;
import com.amazonaws.services.kinesisvideo.model.AckEvent;
import com.amazonaws.services.kinesisvideo.model.FragmentTimecodeType;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.PutMediaRequest;
import com.amazonaws.services.lambda.runtime.Context;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * An example on how to send an MKV file to Kinesis Video Streams.
 *
 * If you have other video formats, you can use ffmpeg to convert to MKV. Only H264 videos are playable in the console.
 * Steps to convert MP4 to MKV:
 *
 * 1. Install ffmpeg if not yet done so already:
 *
 *      Mac OS X:
 *          brew install ffmpeg --with-opus --with-fdk-aac --with-tools --with-freetype --with-libass --with-libvorbis
 *          --with-libvpx --with-x265 --with-libopus
 *
 *      Others:
 *          git clone https://git.ffmpeg.org/ffmpeg.git ffmpeg
 *          ./configure
 *          make
 *          make install
 *
 *  2. Convert MP4 to MKV
 *      ffmpeg -i input.mp4 -b:v 10M -minrate 10M -maxrate 10M -bufsize 10M -bf 0 input.mkv
 */
public final class PutMediaDemo {
    private static final String DEFAULT_REGION = "eu-west-1";

    private static final String STREAM_NAME = "south1";

    private static final String INPUT_STREAM = "http://127.0.0.1:9981/stream/channel/";

    private static final int CONNECTION_TIMEOUT_IN_MILLIS = 10_000;

    private static final DefaultAWSCredentialsProviderChain creds = new DefaultAWSCredentialsProviderChain();

    private static PutMediaDemo runningDemo = null;

    private AmazonKinesisVideoPutMedia dataClient = null;
    private AmazonKinesisVideo frontendClient;
    private String dataEndpoint;

    public static void main(Payload input, Context context) throws Exception {
        if(runningDemo == null){
            System.out.println("runningDemo is NULL, recreating");
            runningDemo = new PutMediaDemo();
            runningDemo.setupResources();
        }

        if(input.getAction().toUpperCase().equals("STOP")){
            runningDemo.stop();
        }else{
            runningDemo.stop();
            runningDemo.start(input.getAction());
        }

        System.out.println("FINISHED REQUEST SUCCESSFULLY");
    }

    public PutMediaDemo() {
        // Do nothing, Lambda calls this then starting the container
    }

    private void setupResources() {
        System.out.println("Setting up PutMediaDemo");

        // Setup auth client
        frontendClient = AmazonKinesisVideoAsyncClient.builder()
                .withCredentials(creds)
                .withRegion(DEFAULT_REGION)
                .build();

        /* this is the endpoint returned by GetDataEndpoint API */
        dataEndpoint = frontendClient.getDataEndpoint(
                new GetDataEndpointRequest()
                        .withStreamName(STREAM_NAME)
                        .withAPIName("PUT_MEDIA")).getDataEndpoint();

    }

    public void stop() throws Exception {
        /* close the client */
        System.out.println("Ensuring stream has stopped");

        if(dataClient != null) {
            dataClient.close();
        }

        dataClient = null;
    }

    public void start(String channelId) {


        InputStream inputStream;
        try {
            // Create a input stream from TVHeadend
            String urlChan = INPUT_STREAM + channelId;
            System.out.println("Starting Stream on " + urlChan);
            URL url = new URL(urlChan);
            inputStream = url.openStream();
        }catch (Exception e){
            System.out.println("An error occurred opening the stream. Aborting. " + e.getMessage());
            return;
        }


        /* PutMedia client */
        dataClient = AmazonKinesisVideoPutMediaClient.builder()
                .withRegion(DEFAULT_REGION)
                .withEndpoint(URI.create(dataEndpoint))
                .withCredentials(creds)
                .withConnectionTimeoutInMillis(CONNECTION_TIMEOUT_IN_MILLIS)
                .build();

        final PutMediaAckResponseHandler responseHandler = new PutMediaAckResponseHandler()  {
            @Override
            public void onAckEvent(AckEvent event) {
                System.out.println("onAckEvent " + event);
            }

            @Override
            public void onFailure(Throwable t) {

                System.out.println("onFailure: " + t.getMessage());
                // TODO: Add your failure handling logic here
            }

            @Override
            public void onComplete() {
                System.out.println("onComplete");

            }
        };

        /* start streaming video in a background thread */
        dataClient.putMedia(new PutMediaRequest()
                        .withStreamName(STREAM_NAME)
                        .withFragmentTimecodeType(FragmentTimecodeType.RELATIVE)
                        .withPayload(inputStream)
                        .withProducerStartTimestamp(Date.from(Instant.now())),
                responseHandler);

        }

}
