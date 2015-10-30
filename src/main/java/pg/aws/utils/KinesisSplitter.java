package pg.aws.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.util.StringUtils;
import lombok.extern.java.Log;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Simple tool to split all the shards of the given AWS Kinesis stream.
 *
 * @author Pedro Gandola <pedro.gandola@pocketmath.com>.
 */
@Log
public class KinesisSplitter {

    private static final BigInteger DENOMINATOR = new BigInteger("2");

    public static void main(String[] args) {
        String streamName = args[0];
        String awsAccessKey = null;
        String awsSecretKey = null;

        if (args.length == 3) {
            awsAccessKey = args[1];
            awsSecretKey = args[2];
        }

        try {
            new KinesisSplitter().split(streamName, awsAccessKey, awsSecretKey, 30);
        } catch (final InterruptedException | AmazonClientException ex) {
            log.log(Level.SEVERE, "Error while splitting the stream: " + streamName, ex);
        }
    }

    public void split(final String streamName, final String awsAccessKey, final String awsSecretKey, long secsToWait)
            throws InterruptedException {

        AWSCredentialsProvider creds = createAwsCredentialsProvider(awsAccessKey, awsSecretKey);
        AmazonKinesisClient client = new AmazonKinesisClient(creds);

        // Describes the stream to get the information about each shard.
        DescribeStreamResult result = client.describeStream(streamName);
        List<Shard> shards = result.getStreamDescription().getShards();

        log.log(Level.INFO, "Splitting the Stream: [{0}], there are [{1}] shards to split.",
                new Object[]{streamName, shards.size()});
        for (final Shard shard : shards) {
            // Gets the new shard start key.
            BigInteger startKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
            BigInteger endKey = new BigInteger(shard.getHashKeyRange().getEndingHashKey());
            String newStartKey = startKey.add(endKey).divide(DENOMINATOR).toString();

            log.log(Level.INFO, "Processing the Shard:[{0}], StartKey:[{1}] EndKey:[{2}] - NewStartKey:[{3}]",
                    new String[]{shard.getShardId(),
                            shard.getHashKeyRange().getStartingHashKey(),
                            shard.getHashKeyRange().getEndingHashKey(),
                            newStartKey});

            // Split the shard.
            client.splitShard(new SplitShardRequest()
                    .withStreamName(streamName)
                    .withShardToSplit(shard.getShardId())
                    .withNewStartingHashKey(newStartKey));

            // Give some time to kinesis to process.
            TimeUnit.SECONDS.sleep(secsToWait);
        }
        log.info("Done!");
    }

    public static AWSCredentialsProvider createAwsCredentialsProvider(final String accessKey, final String secretKey) {
        if (!StringUtils.isNullOrEmpty(accessKey) && StringUtils.isNullOrEmpty(secretKey)) {
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new BasicAWSCredentials(accessKey, secretKey);
                }

                @Override
                public void refresh() {
                }
            };
        }
        return new DefaultAWSCredentialsProviderChain();
    }
}
