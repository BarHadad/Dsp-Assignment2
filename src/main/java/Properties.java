public class Properties {
    public static String keyPair = "imacKey";
    public static String firstJarPath = "s3://2inputs/dsp2.jar";
    public static String firstLogPath = "s3n://logsmr/";

    // step1 - first decade count
    public static String FirstOneGramPath = "s3n://2inputs/1grams";
    public static String FirstTwoGramPath = "s3n://2inputs/2grams";
    public static String FirstOutputPath = "s3n://2outputbucket/FirstCountWithDecadeMR_out";

    // step2 - join be left
    public static String JoinByLeftFirstArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/1gram-r-00000";
    public static String JoinByLeftSecondArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/2gram-r-00000";
    public static String JoinByLeftThirdArg = "s3n://2outputbucket/JoinByLeftInPairMR_out";

    // step3 - join be right
    public static String JoinByRightFirstArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/1gram-r-00000";
    public static String JoinByRightSecondArg = "s3n://2outputbucket/JoinByLeftInPairMR_out/part-r-00000";
    public static String JoinByRightThirdArg = "s3n://2outputbucket/JoinByRightInPairMR_out";

    // step4 - join N
    public static String JoinNFirstArg = "s3n://2outputbucket/FirstCountWithDecadeMR_out/Decs-r-00000";
    public static String JoinNSecondArg = "s3n://2outputbucket/JoinByRightInPairMR_out/part-r-00000";
    public static String JoinNThirdArg = "s3n://2outputbucket/JoinNMR_out";

    // step5 - likelihood
    public static String LikelihoodFirstArg = "s3n://2outputbucket/JoinNMR_out/part-r-00000";
    public static String LikelihoodSecondArg = "s3n://2outputbucket/LikelihoodMR_out";
}
