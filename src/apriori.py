from itertools import combinations


def find_counts(data, subset_length, threshold):
    new_item_sets = data.map(lambda x: [(frozenset(combination), 1) for combination in combinations(x, subset_length)])
    new_item_sets = new_item_sets.flatMap(lambda x: x)
    new_item_sets = new_item_sets.reduceByKey(lambda x, y: x + y)
    new_item_sets = new_item_sets.filter(lambda x: x[1] >= threshold)
    return new_item_sets.collect()


def construct_rules(item_sets_supports, iterations):
    association_rules = []
    for i in range(1, iterations):
        for item_set in item_sets_supports[i]:
            for item_combinations in (combinations(item_set[0], r) for r in range(1, len(item_set[0]))):
                for combination in item_combinations:
                    difference = item_set[0] - set(combination)
                    association_rules.append((frozenset(combination), difference))
    return association_rules


def get_support_dictionary(item_sets_supports, full_count):
    dictionary = dict()
    for i in range(0, len(item_sets_supports)):
        for item_set in item_sets_supports[i]:
            dictionary[frozenset(item_set[0])] = item_set[1] / full_count
    return dictionary


def get_apriori_item_sets(data, iterations, initial_supports, threshold):
    item_sets_supports = [initial_supports]
    for i in range(1, iterations):
        new_item_sets = find_counts(data, i + 1, threshold)
        item_sets_supports.append(new_item_sets)
    return item_sets_supports


def get_apriori_rules_and_dictionary(data, iterations, threshold):
    counts = find_counts(data, 1, threshold)
    print(counts)
    print('calculated initial counts...')
    item_sets = get_apriori_item_sets(data, iterations, counts, threshold)
    print('found item sets...')
    support_dictionary = get_support_dictionary(item_sets, data.count())
    association_rules = construct_rules(item_sets, iterations)
    return association_rules, support_dictionary
