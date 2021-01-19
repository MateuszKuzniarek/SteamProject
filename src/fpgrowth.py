from itertools import combinations, chain

from src.TreeElement import TreeElement


def get_counts(data, threshold):
    new_item_sets = data.map(lambda x: [(element, 1) for element in x])
    new_item_sets = new_item_sets.flatMap(lambda x: x)
    new_item_sets = new_item_sets.reduceByKey(lambda x, y: x + y)
    new_item_sets = new_item_sets.filter(lambda x: x[1] >= threshold)
    return new_item_sets.collect()


def create_tree(data, sorted_counts):
    data_list = data.collect()
    trunk = TreeElement('null', 0, dict())
    for transaction in data_list:
        current_element = trunk
        for item in sorted_counts:
            if item[0] in transaction:
                if item[0] in current_element.children.keys():
                    current_element = current_element.children[item[0]]
                    current_element.counter = current_element.counter + 1
                else:
                    current_element.children[item[0]] = TreeElement(item[0], 1, dict())
                    current_element = current_element.children[item[0]]
    return trunk


def create_conditional_tree(conditional_base):
    trunk = TreeElement('null', 0, dict())
    for transaction in conditional_base:
        current_element = trunk
        for item in transaction[0]:
            if item in current_element.children.keys():
                current_element = current_element.children[item]
                current_element.counter = current_element.counter + transaction[1]
            else:
                current_element.children[item] = TreeElement(item, transaction[1], dict())
                current_element = current_element.children[item]
    return trunk


def construct_conditional_pattern_base(tree, conditional_pattern_base, current_path):
    if len(current_path) > 0:
        if tree.value in conditional_pattern_base.keys():
            conditional_pattern_base[tree.value].append((current_path, tree.counter))
        else:
            conditional_pattern_base[tree.value] = [(current_path, tree.counter)]
    new_path = list(current_path)
    new_path.append(tree.value)
    for child in tree.children:
        construct_conditional_pattern_base(tree.children[child], conditional_pattern_base, new_path)


def mine_conditional_tree(tree, threshold, paths, current_path):
    if tree.counter >= threshold:
        new_path = current_path + [(tree.value, tree.counter)]
        if len(tree.children) > 0:
            for child in tree.children:
                mine_conditional_tree(tree.children[child], threshold, paths, new_path)
        else:
            paths.append(new_path)
    elif len(current_path) > 0:
        paths.append(current_path)


def get_paths_from_pattern_base(conditional_pattern_base, threshold):
    paths_from_pattern_base = dict()
    for element in conditional_pattern_base:
        conditional_tree = create_conditional_tree(conditional_pattern_base[element])
        paths = []
        for child in conditional_tree.children:
            mine_conditional_tree(conditional_tree.children[child], threshold, paths, [])
        paths_from_pattern_base[element] = paths
    return paths_from_pattern_base


def powerset(iterable):
    s = list(iterable)
    subset_length_limit = min(2, len(s))
    return chain.from_iterable(combinations(s, r) for r in range(1, subset_length_limit+1))


def get_item_sets(paths, threshold):
    item_sets = dict()
    for item in paths:
        for path in paths[item]:
            for subset in powerset(path):
                subset_list = list(map(lambda x: x[0], list(subset)))
                item_set_counter = min(map(lambda x: x[1], list(subset)))
                item_set = subset_list + [item]
                if frozenset(item_set) not in item_sets or item_set_counter < item_sets[frozenset(item_set)]:
                    item_sets[frozenset(item_set)] = item_set_counter
    item_sets = dict(filter(lambda x: x[1] >= threshold, item_sets.items()))
    return item_sets


def get_support_dictionary(item_sets_supports, full_count):
    dictionary = dict()
    for item_set in item_sets_supports:
        dictionary[item_set] = item_sets_supports[item_set] / full_count
    return dictionary


def construct_rules(item_sets):
    association_rules = []
    for item_set in item_sets:
        for item_combinations in (combinations(item_set, r) for r in range(1, len(item_set))):
            for combination in item_combinations:
                difference = item_set - set(combination)
                association_rules.append((frozenset(combination), difference))
    return association_rules


def get_fp_growth_rules_and_dictionary(data, threshold):
    counts = get_counts(data, threshold)
    print('calculated counts...')
    counts.sort(key=lambda x: x[1], reverse=True)
    print('sorted counts...')
    tree = create_tree(data, counts)
    print('created tree...')
    conditional_pattern_base = dict()
    for child in tree.children:
        construct_conditional_pattern_base(tree.children[child], conditional_pattern_base, [])
    print('paths found...')
    paths = get_paths_from_pattern_base(conditional_pattern_base, threshold)
    item_sets = get_item_sets(paths, threshold)
    counts = list(map(lambda x: (frozenset([x[0]]), x[1]), counts))
    #all_counts = item_sets.copy()
    #all_counts.update(dict(counts))
    #support_dictionary = get_support_dictionary(all_counts, data.count())
    rules = construct_rules(item_sets)
    return rules
