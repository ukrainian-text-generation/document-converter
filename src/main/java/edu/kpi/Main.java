package edu.kpi;

public class Main {
    public static void main(String[] args) throws Exception {

        String sourceBucketName = args[0];
        String targetBucketName = args[1];
        String projectId = args[2];
        String databaseId = args[3];
        String collectionId = args[4];
        int retries = Integer.parseInt(args[5]);
        int batchSize = Integer.parseInt(args[6]);

        new DocumentConverter(sourceBucketName, targetBucketName, projectId, databaseId, collectionId, retries, batchSize)
                .convertDocuments();
    }
}