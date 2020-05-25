public class Properties {
    public static String keyPair = "imacKey";
    public static String firstJarPath = "s3://2inputs/MapReduce.jar";
    public static String firstLogPath = "s3n://logsmr/";

    // step1 - first decade count
    // for EMR: s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    // s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    public static String FirstOneGramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
    public static String FirstTwoGramPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
    public static String FirstOutputPath = "s3n://2outputbucket/FirstCountWithDecadeMR_out";

    // step2 - join by left in 2-gram
    public static String JoinByLeftFirstArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/1grams";
    public static String JoinByLeftSecondArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/2grams";
    public static String JoinByLeftThirdArg = "s3n://2outputbucket/JoinByLeftInPairMR_out";

    // step3 - join by right in 2-gram
    public static String JoinByRightFirstArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/1grams";
    public static String JoinByRightSecondArg = "s3n://2outputbucket/JoinByLeftInPairMR_out";
    public static String JoinByRightThirdArg = "s3n://2outputbucket/JoinByRightInPairMR_out";

    // step4 - join N
    public static String JoinNFirstArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/Decs";
    public static String JoinNSecondArg = "s3n://2outputbucket/JoinByRightInPairMR_out";
    public static String JoinNThirdArg = "s3n://2outputbucket/JoinNMR_out";

    // step5 - likelihood
    public static String LikelihoodFirstArg = "s3n://2outputbucket/JoinNMR_out";
    public static String LikelihoodSecondArg = "s3n://2outputbucket/LikelihoodMR_out";
}
