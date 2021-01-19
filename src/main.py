from itertools import combinations
from pathlib import Path

from pyspark.sql import SparkSession

from src.apriori import get_apriori_rules_and_dictionary
from src.fpgrowth import get_fp_growth_rules_and_dictionary
from src.measures import get_confidence, get_lift, get_conviction

threshold = 50
apriori_iterations = 3


def get_data_rdd(session, file_name):
    rdd = session.sparkContext.textFile('../resources/' + file_name + '.txt').map(lambda x: x.split(","))
    return rdd


def get_rule_string(antecedent, consequent):
    antecedent_list = list(antecedent)
    consequent_list = list(consequent)
    antecedent_list.sort()
    consequent_list.sort()
    result = ' '.join('<{}>'.format(i) for i in antecedent_list)
    result = result + " [" + ' '.join('<{}>'.format(i) for i in consequent_list) + "] "
    return result


def save_rules(support_dictionary, get_measure, result_file_name, rules):
    association_rules_measures = list(map(lambda x: (x, get_measure(support_dictionary, x[0], x[1]),
                                                     get_rule_string(x[0], x[1])), rules))
    association_rules_measures.sort(key=lambda x: (-x[1], x[2]))
    Path("../resources/").mkdir(parents=True, exist_ok=True)
    for rule_measure in association_rules_measures:
        rule_size = len(rule_measure[0][0]) + len(rule_measure[0][1])
        with open('../resources/' + result_file_name + str(rule_size) + '.txt', 'a') as f:
            f.write(rule_measure[2] + " <" + str(rule_measure[1]) + ">\n")


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    data_rdd = get_data_rdd(spark_session, 'users_genres')
    association_rules, support_dictionary = get_apriori_rules_and_dictionary(data_rdd, apriori_iterations, threshold)
    save_rules(support_dictionary, get_confidence, 'confidence_results_new', association_rules)
    save_rules(support_dictionary, get_lift, 'lift_results_new', association_rules)
    save_rules(support_dictionary, get_conviction, 'conviction_results_new', association_rules)

    association_rules = get_fp_growth_rules_and_dictionary(data_rdd, threshold)
    save_rules(support_dictionary, get_confidence, 'confidence_results_fp', association_rules)
    save_rules(support_dictionary, get_lift, 'lift_results_fp', association_rules)
    save_rules(support_dictionary, get_conviction, 'conviction_results_fp', association_rules)

    print('found rules...')