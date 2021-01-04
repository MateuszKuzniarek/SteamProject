from itertools import combinations
from pathlib import Path

from pyspark.sql import SparkSession

from src.measures import get_confidence, get_lift, get_conviction

threshold = 50
apriori_iterations = 3


def get_data_rdd(session, file_name):
    rdd = session.sparkContext.textFile('../resources/' + file_name + '.txt').map(lambda x: x.split(","))
    return rdd


def find_counts(data, subset_length):
    new_item_sets = data.map(lambda x: [(frozenset(combination), 1) for combination in combinations(x, subset_length)])
    new_item_sets = new_item_sets.flatMap(lambda x: x)
    new_item_sets = new_item_sets.reduceByKey(lambda x, y: x + y)
    new_item_sets = new_item_sets.filter(lambda x: x[1] >= threshold)
    return new_item_sets.collect()


def get_apriori_item_sets(data, iterations, initial_supports):
    item_sets_supports = [initial_supports]
    for i in range(1, iterations):
        new_item_sets = find_counts(data, i + 1)
        item_sets_supports.append(new_item_sets)
    return item_sets_supports


def get_support_dictionary(item_sets_supports, full_count):
    dictionary = dict()
    for i in range(0, len(item_sets_supports)):
        for item_set in item_sets_supports[i]:
            dictionary[frozenset(item_set[0])] = item_set[1] / full_count
    return dictionary


def get_rule_string(antecedent, consequent):
    result = ' '.join('<{}>'.format(i) for i in antecedent)
    result = result + " [" + ' '.join('<{}>'.format(i) for i in consequent) + "] "
    return result


def save_rules(item_sets_supports, iterations, support_dictionary, get_measure, result_file_name):
    for i in range(1, iterations):
        association_rules = []
        for item_set in item_sets_supports[i]:
            for item_combinations in (combinations(item_set[0], r) for r in range(1, len(item_set[0]))):
                for combination in item_combinations:
                    difference = list(item_set[0] - set(combination))
                    measure = get_measure(support_dictionary, item_set, combination, difference)
                    association_rules.append((get_rule_string(combination, difference), measure))

        association_rules.sort(key=lambda x: (-x[1], x[0]))
        Path("../resources/").mkdir(parents=True, exist_ok=True)
        with open('../resources/' + result_file_name + str(i+1) + '.txt', 'w') as f:
            for rule in association_rules:
                f.write(rule[0] + " <" + str(rule[1]) + ">\n")


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    data_rdd = get_data_rdd(spark_session, 'users_genres')
    counts = find_counts(data_rdd, 1)
    print(counts)
    print('calculated initial counts...')
    item_sets = get_apriori_item_sets(data_rdd, apriori_iterations, counts)
    print('found item sets...')
    support_dictionary = get_support_dictionary(item_sets, data_rdd.count())
    save_rules(item_sets, apriori_iterations, support_dictionary, get_confidence, 'confidence_results')
    save_rules(item_sets, apriori_iterations, support_dictionary, get_lift, 'lift_results')
    save_rules(item_sets, apriori_iterations, support_dictionary, get_conviction, 'conviction_results')
    print('found rules...')