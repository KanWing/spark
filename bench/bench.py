
from os import system
import pickle



ALGORITHMS = ["SVM", "SVMADMM", "SVMADMMAsync"]

# class Result:
#     def __init__(self, algorithm, runtime_ms, area_under_pr, training_loss):
#         self.algorithm = algorithm
#         self.runtime_ms = runtime_ms
#         self.area_under_pr = area_under_pr
#         self.training_loss = training_loss

def describe_point_cloud(pointsPerPartition = 100000,
                         partitionSkew = 0.00,
                         labelNoise = 0.2,
                         dimension = 100):
    return   "--pointCloudPointsPerPartition %d " \
             "--pointCloudPartitionSkew %f " \
             "--pointCloudLabelNoise %f " \
             "--pointCloudDimension %d " % \
             (pointsPerPartition,
              partitionSkew,
              labelNoise,
              dimension)

def make_run_cmd(runtimeMS,
                 algorithm,
                 datasetConfigName,
                 datasetConfigStr,
                 regType="L2",
                 regParam=0.0001,
                 numPartitions = 40,

                 miscStr = ""):
    return "cd /mnt/spark; sbin/stop-all.sh; sleep 5; sbin/start-all.sh;" \
           "./bin/spark-submit " \
           "--class org.apache.spark.examples.mllib.research.SynchronousADMMTests" \
           " examples/target/scala-*/spark-examples-*.jar " \
           "--algorithm %s " \
           "--regType %s " \
           "--regParam %f " \
           "--format %s " \
           "--numPartitions %d " \
           "--runtimeMS %d" \
           " %s %s " % \
            (algorithm,
             regType,
             regParam,
             datasetConfigName,
             numPartitions,
             runtimeMS,
             datasetConfigStr,
             miscStr)

def runTest(algorithm, cmd):
    print cmd
    system("eval '%s' > /tmp/run.out 2>&1" % (cmd))

    results = []
    # s"RESULT: ${params.algorithm} \t ${i} \t ${totalTimeMs} \t ${metrics.areaUnderPR()} \t ${metrics.areaUnderROC()} " +
    # s"\t ${trainingLoss} \t  ${regularizationPenalty} \t ${trainingLoss + regularizationPenalty} \t ${model.weights}"
    for line in open("/tmp/run.out"):
        if line.find("RESULT") != -1:
            line = line.split()
            record = {
                "algorithm": algorithm,
                "iterations": int(line[2]),
                "runtime_ms": int(line[3]),
                "training_error": float(line[4]),
                "training_loss": float(line[5]),
                "reg_penalty": float(line[6]),
                "total_loss": line[7],
                "model": line[8],
                "line": line,
                "command": cmd
            }
            results.append(record)

    return results



results = []

for runtime in range(5, 50, 5):
    for algorithm in ALGORITHMS:
        results += runTest(algorithm, make_run_cmd(algorithm, "cloud", describe_point_cloud(),
                                                   iterationStart = runtime, iterationEnd = runtime,
                                                   miscStr="--ADMMmaxLocalIterations 500"))
        # Pickel the output
        output = open('experiment.pkl', 'wb')
        pickle.dump(results, output)
        output.close()

# display the results
print results[0].keys()
for r in results:
    print [r[k] for k in r if k is not "line"]
    
