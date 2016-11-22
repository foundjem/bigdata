from pyspark import SparkContext, SparkConf

def linesToDataset(line):
    (src, dst_line) = line.split('\t')
    src = int(src.strip())

    dst_list_string = dst_line.split(',')
    dst_list = [int(x.strip()) for x in dst_list_string if x != '']

    return (src, dst_list)  

def filterPairs(x):
     # don't take into account pairs of a same node and pairs of already friends
    if (x[0][0] != x[1][0]) and (not x[0][0] in x[1][1]) and (not x[1][0] in x[0][1]):
        shared = len(list(set(x[0][1]).intersection(set(x[1][1]))))
        return (x[0][0], [x[1][0], shared])

def mapFinalDataset(elem):
    recommendations = []
    src = elem[0]
    dst_commons = elem[1]
    for pair in dst_commons:
        if pair[1] > 3: 
            recommendations.append(pair[0])
    return (src, recommendations)

def main():
    conf = SparkConf().setAppName("Recommendation Friendship").setMaster("local[4]")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("data.dat")

    dataset = rdd.map(linesToDataset)

    cartesian = dataset.cartesian(dataset)
    filteredDatasetRaw = cartesian.map(filterPairs)
    filteredDataset = filteredDatasetRaw.filter(lambda x: x != None)
#   print filteredDataset.take(10)

    groupedDataset = filteredDataset.groupByKey().mapValues(list)
#   print groupedDataset.take(10)

    finalDataset = groupedDataset.map(mapFinalDataset)
    output = finalDataset.take(10)
    for (k, v) in output:
        if len(v) > 0:
            print (str(k) + ': ' + str(v))

    sc.stop()


if __name__ == "__main__":
    main()
