public class Properties {
    public static String keyPair = "dsp_key";
    // buckets
    public static String OUT_BUCKET = "s3n://mr-dummy-output";
    public static String IN_BUCKET = "s3n://mr-dummy-input";
    public static String LogsPath = "s3n://mr-dummy-logs";

    public static String firstJarPath = IN_BUCKET + "/MapReduce.jar";
    // step1 - first decade count
    // for EMR:
    // s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    // s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    // dummy data
//    public static String FirstOneGramPath = IN_BUCKET + "/1grams";
//    public static String FirstTwoGramPath = IN_BUCKET + "/2grams";

    public static String FirstOneGramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    public static String FirstTwoGramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    public static String FirstOutputPath = OUT_BUCKET + "/FirstCountWithDecadeMR_out";

    // step2 - join by left in 2-gram
    public static String JoinByLeftFirstArg = OUT_BUCKET + "/FirstCountWithDecadeMR_out/1grams";
    public static String JoinByLeftSecondArg = OUT_BUCKET + "/FirstCountWithDecadeMR_out/2grams";
    public static String JoinByLeftThirdArg = OUT_BUCKET + "/JoinByLeftInPairMR_out";

    // step3 - join by right in 2-gram
    public static String JoinByRightFirstArg = OUT_BUCKET + "/FirstCountWithDecadeMR_out/1grams";
    public static String JoinByRightSecondArg = OUT_BUCKET + "/JoinByLeftInPairMR_out";
    public static String JoinByRightThirdArg = OUT_BUCKET + "/FirstCountWithDecadeMR_out/Decs";
    public static String JoinByRightFourthArg = OUT_BUCKET + "/JoinByRightInPairMR_out";

    // step4 - likelihood
    public static String LikelihoodFirstArg = OUT_BUCKET + "/JoinByRightInPairMR_out";
    public static String LikelihoodSecondArg = OUT_BUCKET + "/LikelihoodMR_out";
}