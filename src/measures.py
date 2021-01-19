def get_confidence(support_dictionary, combination, difference):
    item_set = combination.union(difference)
    confidence = support_dictionary[item_set] / support_dictionary[frozenset(combination)]
    return confidence


def get_lift(support_dictionary, combination, difference):
    item_set = combination.union(difference)
    lift = support_dictionary[item_set] / \
           (support_dictionary[frozenset(combination)] * support_dictionary[frozenset(difference)])
    return lift


def get_conviction(support_dictionary, combination, difference):
    confidence = get_confidence(support_dictionary, combination, difference)
    if confidence != 1:
        conviction = (1 - support_dictionary[frozenset(difference)]) / (1 - confidence)
    else:
        conviction = 0
    return conviction
